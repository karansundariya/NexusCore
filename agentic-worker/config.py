"""
=============================================================================
config.py — Agentic Worker Configuration
=============================================================================
Loads all configuration from environment variables with sensible defaults.
Use a .env file for local development; use secrets manager in production.

WHY ENVIRONMENT VARIABLES?
  The Twelve-Factor App methodology (12factor.net) requires configuration to
  be stored in the environment, not in code. This allows the same Docker image
  to run in dev/staging/production with different configs injected at runtime.

USAGE:
  Copy .env.example to .env and fill in your values.
  The worker reads these via python-dotenv on startup.
=============================================================================
"""

import os
from dotenv import load_dotenv

# Load .env file if present (for local development)
# In production (Docker/K8s), environment variables are injected directly
load_dotenv()


class Config:
    """
    Centralized configuration object for the agentic worker.

    All configuration values are read once at startup from environment variables.
    Using a class (rather than module-level variables) allows easy mocking in tests.
    """

    # =========================================================================
    # Kafka Configuration
    # =========================================================================
    # Bootstrap servers for the Kafka cluster
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    # Topics
    KAFKA_TOPIC_PROMPTS: str = os.getenv("KAFKA_TOPIC_PROMPTS", "ai.prompts.unprocessed")
    KAFKA_TOPIC_TELEMETRY: str = os.getenv("KAFKA_TOPIC_TELEMETRY", "ai.telemetry.events")

    # Consumer group ID — all workers in the same group share the workload
    # Each Kafka partition is assigned to exactly one consumer in the group
    KAFKA_CONSUMER_GROUP: str = os.getenv("KAFKA_CONSUMER_GROUP", "nexuscore-agentic-workers")

    # =========================================================================
    # Redis Configuration
    # =========================================================================
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))

    # VSS index name (must match the Java gateway config)
    REDIS_VSS_INDEX: str = os.getenv("REDIS_VSS_INDEX", "idx:prompt_vss")

    # Cache entry TTL in seconds: 24 hours
    CACHE_TTL_SECONDS: int = int(os.getenv("CACHE_TTL_SECONDS", "86400"))

    # =========================================================================
    # LLM Model Configuration (LiteLLM)
    # =========================================================================
    # Primary model: tried first for every request
    # WHY CLAUDE 3.5 SONNET as default?
    #   It has the best code/reasoning performance on MMLU benchmarks (2024)
    #   at a cost below GPT-4o. Anthropic's API has 99.9% uptime SLA.
    PRIMARY_MODEL: str = os.getenv("PRIMARY_MODEL", "claude-3-5-sonnet-20241022")

    # Secondary fallback: used when primary returns 429 (rate limit) or 500 (error)
    # WHY GEMINI 1.5 FLASH as fallback?
    #   It's the fastest (sub-second TTFT) and cheapest high-quality model,
    #   making it ideal for overflow traffic that must be served quickly.
    FALLBACK_MODEL: str = os.getenv("FALLBACK_MODEL", "gemini/gemini-1.5-flash")

    # Tertiary fallback: if both primary and secondary fail
    TERTIARY_MODEL: str = os.getenv("TERTIARY_MODEL", "mistral/mistral-large-latest")

    # Request timeout: if LLM doesn't respond within this time, trigger fallback
    LLM_TIMEOUT_SECONDS: int = int(os.getenv("LLM_TIMEOUT_SECONDS", "30"))

    # =========================================================================
    # API Keys
    # =========================================================================
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")
    GOOGLE_API_KEY: str = os.getenv("GOOGLE_API_KEY", "")
    MISTRAL_API_KEY: str = os.getenv("MISTRAL_API_KEY", "")

    # =========================================================================
    # Gateway Configuration
    # =========================================================================
    # URL where the worker sends completed responses back to the Gateway
    GATEWAY_BASE_URL: str = os.getenv("GATEWAY_BASE_URL", "http://localhost:8080")
    GATEWAY_INTERNAL_ENDPOINT: str = "/v1/internal/complete"

    # =========================================================================
    # Observability
    # =========================================================================
    # OpenTelemetry collector endpoint (Jaeger/Cloud Trace/etc.)
    OTEL_EXPORTER_OTLP_ENDPOINT: str = os.getenv(
        "OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"
    )
    SERVICE_NAME: str = "nexuscore-agentic-worker"

    # =========================================================================
    # Worker Settings
    # =========================================================================
    # Number of concurrent threads for processing Kafka messages
    # WHY THREADS OVER ASYNC?
    #   LiteLLM and kafka-python use blocking I/O. Using a thread pool allows
    #   multiple Kafka messages to be processed in parallel without converting
    #   the entire codebase to async/await.
    WORKER_THREADS: int = int(os.getenv("WORKER_THREADS", "4"))

    # Poll timeout: how long the Kafka consumer waits for new messages before
    # checking for shutdown signals. Smaller = more responsive shutdown.
    KAFKA_POLL_TIMEOUT_MS: int = int(os.getenv("KAFKA_POLL_TIMEOUT_MS", "1000"))

    # Enable mock LLM mode: returns a canned response without calling any LLM API
    # Useful for local testing without API keys
    USE_MOCK_LLM: bool = os.getenv("USE_MOCK_LLM", "true").lower() == "true"

    # Embedding dimensions — must match the Java gateway and Redis VSS index
    EMBEDDING_DIMENSIONS: int = int(os.getenv("EMBEDDING_DIMENSIONS", "1536"))


# Singleton instance used throughout the worker
config = Config()
