"""
=============================================================================
main.py — NexusCore Agentic Worker Entry Point
=============================================================================
WHAT THIS FILE DOES:
  The main entry point for the Python LangGraph agentic worker. It:
  1. Initializes all services (Kafka consumer, Redis cache writer, LangGraph graph)
  2. Starts a Kafka consumer loop polling `ai.prompts.unprocessed`
  3. For each message: runs the LangGraph multi-agent pipeline
  4. After pipeline completion: delivers the response to the waiting Gateway client
  5. Writes the response to the Redis semantic cache asynchronously
  6. Publishes a TelemetryEvent to `ai.telemetry.events` for audit logging

CONCURRENCY MODEL:
  The Kafka consumer loop runs on the main thread.
  Each message is dispatched to a ThreadPoolExecutor (WORKER_THREADS = 4).
  This allows 4 prompts to be processed simultaneously without blocking the
  Kafka consumer loop (which must poll regularly to avoid session timeout).

  WHY THREADS OVER ASYNC/AWAIT?
    LiteLLM and the openai SDK use blocking HTTP calls internally. While async
    versions exist, the LangGraph framework (as of v0.2) uses synchronous
    execution for its graph.invoke() call. Using a thread pool is the simplest
    correct approach for concurrent I/O-bound work with blocking libraries.

KAFKA CONSUMER CONFIGURATION:
  group.id: nexuscore-agentic-workers
    All worker instances in the same consumer group share the work.
    With 3 Kafka partitions and 3 worker pods in Kubernetes, each pod
    gets exactly 1 partition assigned — perfect horizontal scaling.

  enable.auto.commit: False
    Manual offset commit: we commit ONLY after successfully sending the
    response to the Gateway. This guarantees at-least-once processing.
    In the worst case (worker crash after sending but before committing),
    the message is redelivered and a duplicate response is sent to the Gateway.
    The Gateway's idempotent Sinks.One<> handle duplicates gracefully.

  auto.offset.reset: earliest
    New consumer groups start from the oldest available message.
    This ensures no messages are lost if a new worker deployment connects
    to a topic with existing unconsumed messages.

SHUTDOWN HANDLING:
  The worker registers SIGINT/SIGTERM handlers to gracefully drain in-flight
  requests before exiting. This ensures:
  1. In-flight LLM calls complete and responses are delivered
  2. Redis cache writes finish
  3. Kafka offsets are committed for completed messages
  4. The Kafka consumer sends a LeaveGroup request (triggers immediate rebalance)
=============================================================================
"""

import json
import logging
import os
import selectors
import signal
import sys
import time

# Windows Python 3.12 selector compatibility patch for kafka-python
if sys.platform == "win32" and hasattr(selectors, "SelectSelector"):
    def _safe_select_unregister(self, fileobj):
        key = None
        try:
            fd = self._fileobj_lookup(fileobj)
            key = self._fd_to_key.pop(fd, None)
        except Exception:
            for fd, k in list(self._fd_to_key.items()):
                if k.fileobj is fileobj:
                    key = self._fd_to_key.pop(fd, None)
                    break
        if key:
            self._readers.discard(key.fd)
            self._writers.discard(key.fd)
        return key

    selectors.SelectSelector.unregister = _safe_select_unregister
from concurrent.futures import ThreadPoolExecutor, Future
from threading import Event
from typing import Optional

import httpx
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from cache_writer import CacheWriter
from config import config
from graph import get_agent_graph, AgentState

# =============================================================================
# Logging Setup
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(levelname)s %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# =============================================================================
# Main Worker Class
# =============================================================================

class AgenticWorker:
    """
    The main NexusCore agentic worker.

    Orchestrates the full pipeline:
      Kafka consume → LangGraph invoke → Gateway deliver → Redis cache → Kafka telemetry
    """

    def __init__(self):
        """Initialize all worker components."""
        logger.info("🚀 Initializing NexusCore Agentic Worker...")
        logger.info("  Config: primary_model=%s, mock_llm=%s, threads=%d",
                   config.PRIMARY_MODEL, config.USE_MOCK_LLM, config.WORKER_THREADS)

        # Shutdown event: set to True to trigger graceful shutdown
        self.shutdown_event = Event()

        # Thread pool for concurrent prompt processing
        # max_workers = number of simultaneous LLM calls
        self.executor = ThreadPoolExecutor(
            max_workers=config.WORKER_THREADS,
            thread_name_prefix="nexus-worker"
        )

        # Redis cache writer for populating the semantic cache
        self.cache_writer = CacheWriter()

        # HTTP client for delivering responses back to the Java Gateway
        # Timeouts: connect=5s, read=10s (the Gateway should respond immediately)
        self.http_client = httpx.Client(
            timeout=httpx.Timeout(connect=5.0, read=10.0, write=5.0, pool=5.0)
        )

        # Initialize Kafka consumer
        self.consumer = self._create_kafka_consumer()

        # Initialize Kafka producer (for telemetry events)
        self.producer = self._create_kafka_producer()

        # Build the LangGraph compiled graph (singleton, built once)
        self.agent_graph = get_agent_graph()

        logger.info("✓ NexusCore Agentic Worker initialized successfully")

    def _create_kafka_consumer(self) -> KafkaConsumer:
        """
        Creates a Kafka consumer for the `ai.prompts.unprocessed` topic.

        KEY CONFIG DECISIONS:
          enable_auto_commit=False: Manual offset commit for at-least-once delivery
          value_deserializer: JSON → dict conversion for PromptEvent messages
          session_timeout_ms=30000: Consumer must poll within 30s or be kicked from group
          max_poll_records=1: Process one message at a time per poll cycle
            WHY 1? Each LLM call takes 1-30 seconds. If we fetched 10 messages
            in one poll, the consumer might not poll again for 30+ seconds,
            triggering a session timeout and partition rebalance.
        """
        logger.info("Creating Kafka consumer: servers=%s, topic=%s, group=%s",
                   config.KAFKA_BOOTSTRAP_SERVERS, config.KAFKA_TOPIC_PROMPTS,
                   config.KAFKA_CONSUMER_GROUP)
        try:
            consumer = KafkaConsumer(
                config.KAFKA_TOPIC_PROMPTS,
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                api_version=(2, 6, 0),
                group_id=config.KAFKA_CONSUMER_GROUP,
                enable_auto_commit=False,          # Manual offset control
                auto_offset_reset="earliest",       # Start from beginning for new groups
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
                session_timeout_ms=30000,           # 30s session timeout
                heartbeat_interval_ms=10000,        # Heartbeat every 10s
                max_poll_records=1,                 # One message per poll (safe for long LLM calls)
                max_poll_interval_ms=120000,        # Max 2 minutes between polls (covers long LLM calls)
                fetch_max_bytes=10485760,           # 10MB max fetch (for large prompts)
            )
            logger.info("✓ Kafka consumer connected")
            return consumer
        except KafkaError as e:
            logger.error("Failed to create Kafka consumer: %s", str(e))
            raise

    def _create_kafka_producer(self) -> KafkaProducer:
        """
        Creates a Kafka producer for the `ai.telemetry.events` topic.
        Used to publish telemetry events after each request completes.
        """
        logger.info("Creating Kafka producer: servers=%s", config.KAFKA_BOOTSTRAP_SERVERS)
        try:
            producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                api_version=(2, 6, 0),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",                        # Wait for all replicas to acknowledge
                retries=5,                          # Retry transient send failures
                max_in_flight_requests_per_connection=1,  # Required for ordering guarantees
            )
            logger.info("✓ Kafka producer connected")
            return producer
        except KafkaError as e:
            logger.error("Failed to create Kafka producer: %s", str(e))
            raise

    def run(self) -> None:
        """
        Main consumer loop. Polls Kafka and dispatches messages to worker threads.

        LOOP BEHAVIOR:
          1. Poll Kafka for new messages (blocks for up to KAFKA_POLL_TIMEOUT_MS)
          2. For each message in the poll batch (max 1 per config):
             a. Submit it to the thread pool for async processing
             b. Attach a callback to commit the offset on success
          3. Check shutdown_event — exit gracefully if set

        GRACEFUL SHUTDOWN:
          When SIGTERM/SIGINT is received, shutdown_event is set.
          The loop exits after the current poll cycle completes.
          The ThreadPoolExecutor.shutdown(wait=True) call then waits for all
          in-flight requests to complete before the process exits.
        """
        logger.info("🎯 Starting Kafka consumer loop [topic=%s]...", config.KAFKA_TOPIC_PROMPTS)
        logger.info("   Listening for prompts. Press Ctrl+C to stop.")

        while not self.shutdown_event.is_set():
            try:
                # Poll Kafka for new messages
                # Returns a dict: {TopicPartition → [ConsumerRecord, ...]}
                records = self.consumer.poll(
                    timeout_ms=config.KAFKA_POLL_TIMEOUT_MS,
                    max_records=1,  # Match max_poll_records setting
                )

                for partition, messages in records.items():
                    for message in messages:
                        prompt_event = message.value
                        request_id = prompt_event.get("requestId", "unknown")

                        logger.info(
                            "📨 Received prompt [%s]: partition=%d, offset=%d",
                            request_id, message.partition, message.offset
                        )

                        # Submit processing to thread pool
                        future: Future = self.executor.submit(
                            self._process_prompt,
                            prompt_event,
                            message,
                        )

                        # Add callback to log result (not used for offset commit —
                        # offset is committed inside _process_prompt on success)
                        future.add_done_callback(
                            lambda f, rid=request_id: self._on_processing_complete(f, rid)
                        )

            except Exception as e:
                if not self.shutdown_event.is_set():
                    logger.error("Consumer loop error: %s", str(e), exc_info=True)
                    time.sleep(1)  # Brief pause before retry to avoid tight error loops

        logger.info("⏹️  Shutdown signal received. Draining in-flight requests...")

    def _process_prompt(self, prompt_event: dict, kafka_message) -> dict:
        """
        The core processing function executed per Kafka message in a worker thread.

        FULL PIPELINE:
          1. Build LangGraph initial state from the Kafka PromptEvent
          2. Invoke the compiled LangGraph graph (classification → tools? → guardrail)
          3. Extract the final validated response from the graph state
          4. Deliver the response to the waiting Gateway client via HTTP
          5. Write the response to Redis semantic cache (background)
          6. Publish telemetry event to Kafka ai.telemetry.events
          7. Commit the Kafka offset (marks message as processed)

        @param prompt_event: The deserialized PromptEvent JSON dict
        @param kafka_message: The raw KafkaConsumer record (for offset commit)
        @returns: Result dict with response and telemetry data
        """
        request_id = prompt_event.get("requestId", "unknown")
        start_time_ms = int(time.time() * 1000)

        logger.info("⚙️  Processing request [%s]", request_id)

        try:
            # =====================================================================
            # Step 1: Build the initial LangGraph AgentState
            # =====================================================================
            prompt_text = prompt_event.get("promptText", "")
            model = prompt_event.get("model", config.PRIMARY_MODEL)
            temperature = float(prompt_event.get("temperature", 0.7))
            max_tokens = prompt_event.get("maxTokens")
            trace_id = prompt_event.get("traceId")
            prompt_vector_b64 = prompt_event.get("promptVectorBase64")

            # Build the messages list for the LLM
            # Include a system prompt that sets up the assistant persona
            messages = [
                {
                    "role": "system",
                    "content": (
                        "You are NexusCore, an enterprise AI assistant. "
                        "Provide accurate, helpful, and concise responses. "
                        "If you need data from internal systems, it will be provided to you."
                    )
                },
                {
                    "role": "user",
                    "content": prompt_text,
                }
            ]

            # Build the initial state for the LangGraph graph
            initial_state: AgentState = {
                "request_id": request_id,
                "messages": messages,
                "intent": None,
                "tool_results": None,
                "final_response": None,
                "model_used": None,
                "input_tokens": 0,
                "output_tokens": 0,
                "retry_count": 0,
                "fallback_used": False,
                "trace_id": trace_id,
            }

            # =====================================================================
            # Step 2: Run the LangGraph multi-agent pipeline
            # =====================================================================
            logger.info("  Running LangGraph pipeline [%s]...", request_id)
            final_state = self.agent_graph.invoke(initial_state)

            response_text = final_state.get("final_response", "")
            model_used = final_state.get("model_used", "unknown")
            input_tokens = final_state.get("input_tokens", 0)
            output_tokens = final_state.get("output_tokens", 0)
            fallback_used = final_state.get("fallback_used", False)

            if not response_text:
                response_text = "I'm sorry, I was unable to generate a response. Please try again."
                model_used = "error"

            logger.info(
                "  ✓ Pipeline complete [%s]: model=%s, tokens=%d/%d, fallback=%s",
                request_id, model_used, input_tokens, output_tokens, fallback_used,
            )

            # =====================================================================
            # Step 3: Deliver response back to the waiting Gateway client
            # =====================================================================
            self._deliver_response_to_gateway(request_id, response_text)

            # =====================================================================
            # Step 4: Populate Redis semantic cache
            # =====================================================================
            # Run cache write AFTER delivering the response (non-blocking for client)
            self.cache_writer.write_cache_entry(
                prompt_text=prompt_text,
                response_text=response_text,
                model_used=model_used,
                request_id=request_id,
                prompt_vector_base64=prompt_vector_b64,
            )

            # =====================================================================
            # Step 5: Publish telemetry event to Kafka
            # =====================================================================
            latency_ms = int(time.time() * 1000) - start_time_ms
            self._publish_telemetry(
                request_id=request_id,
                api_key_hash=prompt_event.get("apiKeyHash"),
                prompt_text=prompt_text,
                model_used=model_used,
                cache_status="MISS",
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                latency_ms=latency_ms,
                trace_id=trace_id,
                http_status=200,
            )

            # =====================================================================
            # Step 6: Commit Kafka offset — mark this message as processed
            # =====================================================================
            # We commit HERE (not auto-commit) because we've:
            # a) Delivered the response to the client ✓
            # b) Written to Redis cache ✓
            # c) Published telemetry ✓
            # Only now is it safe to move the offset forward
            self.consumer.commit()
            logger.info("✓ Kafka offset committed [%s]", request_id)

            return {
                "request_id": request_id,
                "model_used": model_used,
                "latency_ms": latency_ms,
                "fallback_used": fallback_used,
            }

        except Exception as e:
            logger.error("❌ Failed to process request [%s]: %s", request_id, str(e), exc_info=True)
            # DO NOT commit offset on failure — Kafka will redeliver to another worker
            raise

    def _deliver_response_to_gateway(self, request_id: str, response_text: str) -> None:
        """
        Sends the generated response back to the Java Gateway via HTTP.

        The Gateway has a Sinks.One<> registered for this requestId.
        When we POST to /v1/internal/complete?requestId=<id>, the Gateway
        resolves the Sink and returns the response to the waiting client.

        FAILURE HANDLING:
          If the Gateway is unreachable (pod restart, network issue), we log
          the error. The client will receive a 60-second timeout response.
          The Kafka offset is NOT committed, so the message will be redelivered
          — but by then the client's Mono has already timed out, so the
          redelivered response goes nowhere (the Gateway returns 404 for the Sink).

        @param request_id: The requestId the Gateway is waiting on
        @param response_text: The generated LLM response to deliver
        """
        endpoint = f"{config.GATEWAY_BASE_URL}{config.GATEWAY_INTERNAL_ENDPOINT}"
        params = {"requestId": request_id}

        logger.info("📤 Delivering response to Gateway [%s]: %d chars", request_id, len(response_text))

        try:
            response = self.http_client.post(
                endpoint,
                params=params,
                content=response_text.encode("utf-8"),
                headers={"Content-Type": "text/plain; charset=utf-8"},
            )

            if response.status_code == 200:
                logger.info("✓ Response delivered to Gateway [%s]", request_id)
            elif response.status_code == 404:
                logger.warning(
                    "⚠️  Gateway has no pending Sink for [%s] — client may have timed out",
                    request_id
                )
            else:
                logger.error(
                    "✗ Unexpected Gateway response [%s]: HTTP %d — %s",
                    request_id, response.status_code, response.text
                )
        except httpx.RequestError as e:
            logger.error(
                "✗ Failed to deliver to Gateway [%s]: %s",
                request_id, str(e)
            )

    def _publish_telemetry(
        self,
        request_id: str,
        api_key_hash: Optional[str],
        prompt_text: str,
        model_used: str,
        cache_status: str,
        input_tokens: int,
        output_tokens: int,
        latency_ms: int,
        trace_id: Optional[str],
        http_status: int,
        cosine_similarity: Optional[float] = None,
    ) -> None:
        """
        Publishes a TelemetryEvent to the `ai.telemetry.events` Kafka topic.
        The Java Gateway's KafkaConsumerService consumes this and writes it
        to the PostgreSQL audit log.

        @param request_id: Unique request ID
        @param ... other telemetry fields
        """
        telemetry_event = {
            "requestId": request_id,
            "apiKeyHash": api_key_hash,
            "promptText": prompt_text[:500] if prompt_text else None,  # Truncate for Kafka size
            "modelUsed": model_used,
            "cacheStatus": cache_status,
            "token_count_input": input_tokens,
            "token_count_output": output_tokens,
            "latency_ms": latency_ms,
            "cosine_similarity": cosine_similarity,
            "traceId": trace_id,
            "http_status_code": http_status,
            "created_at": int(time.time() * 1000),
        }

        try:
            self.producer.send(
                config.KAFKA_TOPIC_TELEMETRY,
                key=request_id,
                value=telemetry_event,
            )
            self.producer.flush(timeout=5)  # Wait for broker ACK (max 5s)
            logger.debug("✓ Telemetry published [%s]", request_id)
        except Exception as e:
            # Telemetry failures are non-fatal — don't affect the client response
            logger.warning("⚠️  Telemetry publish failed [%s]: %s", request_id, str(e))

    def _on_processing_complete(self, future: Future, request_id: str) -> None:
        """Callback: logs whether the future completed successfully or raised an exception."""
        if future.exception():
            logger.error("Worker thread failed [%s]: %s", request_id, str(future.exception()))
        else:
            logger.debug("Worker thread completed successfully [%s]", request_id)

    def shutdown(self) -> None:
        """
        Graceful shutdown: stop consuming, drain in-flight requests, close connections.
        Called when SIGTERM or SIGINT is received.
        """
        logger.info("🛑 Initiating graceful shutdown...")
        self.shutdown_event.set()

        # Wait for all in-flight processing tasks to complete
        self.executor.shutdown(wait=True, cancel_futures=False)
        logger.info("✓ All in-flight requests drained")

        # Close Kafka connections
        self.consumer.close()
        self.producer.close()
        logger.info("✓ Kafka connections closed")

        # Close HTTP client
        self.http_client.close()
        logger.info("✓ HTTP client closed")

        logger.info("✅ NexusCore Agentic Worker shut down cleanly")


# =============================================================================
# Entry Point
# =============================================================================

def main():
    """
    Main entry point: initializes the worker and starts the consumer loop.
    Registers signal handlers for graceful shutdown.
    """
    logger.info("=" * 60)
    logger.info(" NexusCore Agentic Worker v1.0.0")
    logger.info(" Python %s", sys.version.split()[0])
    logger.info(" PID: %d", os.getpid())
    logger.info("=" * 60)

    worker = AgenticWorker()

    # Register SIGTERM handler (used by Docker/Kubernetes for graceful shutdown)
    # and SIGINT handler (Ctrl+C in development)
    def handle_signal(signum, frame):
        logger.info("Received signal %d — initiating graceful shutdown", signum)
        worker.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    # Start the consumer loop (blocks until shutdown)
    worker.run()

    # Cleanup after loop exits (should be handled by handle_signal, but just in case)
    worker.shutdown()


if __name__ == "__main__":
    main()
