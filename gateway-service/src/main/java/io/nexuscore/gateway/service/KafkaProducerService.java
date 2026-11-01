package io.nexuscore.gateway.service;

import io.nexuscore.gateway.model.PromptEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.CompletableFuture;

/**
 * =============================================================================
 * KafkaProducerService — Async Prompt Event Publisher
 * =============================================================================
 * WHAT THIS SERVICE DOES:
 *   On a semantic cache miss, publishes the incoming prompt as a PromptEvent
 *   to the Kafka topic `ai.prompts.unprocessed`. The Python LangGraph worker
 *   consumes this event, runs the multi-agent pipeline, and returns the
 *   generated response.
 *
 * WHY KAFKA FOR DECOUPLING?
 *   The Gateway cannot wait synchronously for LLM generation (can take 1-60s).
 *   Publishing to Kafka decouples the HTTP connection (held open via SSE) from
 *   the actual LLM execution. Benefits:
 *   1. TRAFFIC SPIKE BUFFERING: If 1000 requests arrive simultaneously, they're
 *      queued in Kafka. Workers consume at their own pace without overloading.
 *   2. HORIZONTAL SCALING: Add more Python workers by scaling consumer group members.
 *      Kafka automatically rebalances partition assignments.
 *   3. DURABILITY: If a worker crashes mid-generation, the uncommitted Kafka offset
 *      causes Kafka to redeliver the message to another worker (at-least-once delivery).
 *   4. OBSERVABILITY: Kafka acts as an audit log of every request. Replay from any offset.
 *
 * PRODUCER DURABILITY SETTINGS (in application.yml):
 *   acks=all:             Wait for ALL in-sync replicas to acknowledge
 *   enable.idempotence=true: Exactly-once semantics (no duplicate messages on retry)
 *   retries=5:            Retry transient network failures
 *   retry.backoff.ms=500: 500ms between retries (exponential backoff built in)
 *
 * MESSAGE KEYING STRATEGY:
 *   Message key = requestId (UUID). Kafka partitions by key hash (murmur2).
 *   WHY KEY BY requestId?
 *   - All messages for the same request go to the same partition
 *   - Within a partition, messages are strictly ordered by offset
 *   - This guarantees that retry messages for the same request are processed
 *     in order (though requestIds should be unique per request in practice)
 *
 * REACTIVE INTEGRATION:
 *   KafkaTemplate.send() returns a CompletableFuture. We wrap it in Mono using
 *   Mono.fromFuture() and run it on the boundedElastic scheduler to avoid
 *   blocking the Netty I/O thread during the Kafka producer flush.
 * =============================================================================
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaProducerService {

    /** Spring Kafka template for publishing PromptEvent messages */
    private final KafkaTemplate<String, PromptEvent> kafkaTemplate;

    @Value("${nexuscore.kafka.topics.prompts-unprocessed:ai.prompts.unprocessed}")
    private String promptsTopic;

    /**
     * Publishes a cache-miss prompt event to Kafka for async LLM processing.
     *
     * <p>The method is reactive: it returns a Mono<SendResult> that completes
     * when Kafka acknowledges the write (after acks=all replication).
     *
     * <p>FAILURE HANDLING:
     *   If Kafka is unavailable, the Mono errors with a KafkaException.
     *   The caller (ChatCompletionsController) handles this by returning HTTP 503.
     *   The Gateway does NOT retry in-process — Kafka's producer retry config
     *   handles transient network failures at the broker level.
     *
     * @param event The PromptEvent to publish (prompt text, model, embeddings, etc.)
     * @return Mono<SendResult> completing when Kafka acknowledges the write
     */
    public Mono<SendResult<String, PromptEvent>> publishPromptEvent(PromptEvent event) {
        log.info("📤 Publishing prompt to Kafka [topic={}, requestId={}, model={}]",
                promptsTopic, event.getRequestId(), event.getModel());

        // KafkaTemplate.send() is internally synchronous but uses a CompletableFuture
        // for the broker acknowledgment. We wrap in Mono.fromFuture() to make it
        // composable in the reactive WebFlux pipeline.
        CompletableFuture<SendResult<String, PromptEvent>> future =
                kafkaTemplate.send(
                        promptsTopic,
                        event.getRequestId(),  // Message key = requestId for partition consistency
                        event
                );

        return Mono.fromFuture(future)
                // Run on boundedElastic: Kafka send involves blocking I/O internally
                .subscribeOn(Schedulers.boundedElastic())
                .doOnSuccess(result -> {
                    var metadata = result.getRecordMetadata();
                    log.info("✓ Kafka ACK: topic={}, partition={}, offset={}, requestId={}",
                            metadata.topic(),
                            metadata.partition(),
                            metadata.offset(),
                            event.getRequestId());
                })
                .doOnError(error -> log.error("✗ Kafka send FAILED: requestId={}, error={}",
                        event.getRequestId(), error.getMessage()));
    }
}
