package io.nexuscore.gateway.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * =============================================================================
 * TelemetryEvent — Kafka Message Schema for ai.telemetry.events
 * =============================================================================
 * WHAT THIS IS:
 *   Published by the Python LangGraph worker after completing each request.
 *   Consumed by KafkaConsumerService and persisted to the llm_audit_logs table.
 *
 * FIELDS MAP 1:1 to llm_audit_logs columns.
 * =============================================================================
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TelemetryEvent {

    private String requestId;
    private String apiKeyHash;
    private String promptText;
    private String modelUsed;
    private String cacheStatus;

    @JsonProperty("token_count_input")
    private Integer tokenCountInput;

    @JsonProperty("token_count_output")
    private Integer tokenCountOutput;

    @JsonProperty("latency_ms")
    private Integer latencyMs;

    @JsonProperty("cosine_similarity")
    private Double cosineSimilarity;

    private String traceId;

    @JsonProperty("http_status_code")
    private Integer httpStatusCode;

    @Builder.Default
    @JsonProperty("created_at")
    private long createdAt = Instant.now().toEpochMilli();
}
