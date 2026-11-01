package io.nexuscore.gateway.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.UUID;

/**
 * =============================================================================
 * PromptEvent — Kafka Message Schema for ai.prompts.unprocessed
 * =============================================================================
 * WHAT THIS IS:
 *   The serialized message published to Kafka when a cache miss occurs.
 *   The Python LangGraph worker consumes this event, processes it through
 *   the multi-agent pipeline, and returns a response via the reactive stream.
 *
 * DESIGN DECISIONS:
 *   - All fields are nullable to support partial population at different stages.
 *   - requestId links this event to the client's HTTP connection held open by SSE.
 *   - createdAt enables TTL calculation: discard events older than 30 seconds
 *     (the client will have already timed out and reconnected).
 *
 * KAFKA SERIALIZATION:
 *   Published as JSON via Spring Kafka's JsonSerializer.
 *   Message key = requestId (ensures ordering: all retries for same request
 *   go to the same Kafka partition, preserving causal ordering).
 * =============================================================================
 */
@Data
@Builder
public class PromptEvent {

    /** Globally unique request identifier. Used as Kafka message key. */
    @Builder.Default
    private String requestId = UUID.randomUUID().toString();

    /** SHA-256 hash of the API key that initiated this request */
    private String apiKeyHash;

    /** The full original prompt text (last user message) */
    private String promptText;

    /** The full messages array as JSON (for multi-turn context) */
    private String messagesJson;

    /** Target model requested by the client (e.g., "gpt-4o") */
    private String model;

    /** Sampling temperature from the request */
    private Double temperature;

    /** Maximum tokens from the request */
    private Integer maxTokens;

    /** The 1536-dim vector embedding of the prompt (base64 encoded) */
    private String promptVectorBase64;

    /** Whether to stream the response back */
    @Builder.Default
    private Boolean stream = false;

    /** OpenTelemetry trace ID for distributed tracing correlation */
    private String traceId;

    /** Unix epoch millis when this event was created */
    @Builder.Default
    @JsonProperty("created_at")
    private long createdAt = Instant.now().toEpochMilli();
}
