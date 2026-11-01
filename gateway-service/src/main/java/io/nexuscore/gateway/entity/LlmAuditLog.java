package io.nexuscore.gateway.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * =============================================================================
 * LlmAuditLog — JPA Entity for llm_audit_logs Table
 * =============================================================================
 * Maps to the PostgreSQL table created in init_postgres.sql.
 * Written by KafkaConsumerService whenever a TelemetryEvent is received.
 *
 * This entity is IMMUTABLE after creation — no update methods.
 * The append-only audit log pattern ensures compliance and forensic integrity.
 * =============================================================================
 */
@Entity
@Table(name = "llm_audit_logs")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LlmAuditLog {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "request_id", nullable = false, length = 36)
    private String requestId;

    @Column(name = "api_key_hash", length = 64)
    private String apiKeyHash;

    @Column(name = "prompt_text", columnDefinition = "TEXT")
    private String promptText;

    @Column(name = "model_used", nullable = false, length = 100)
    private String modelUsed;

    @Column(name = "cache_status", nullable = false, length = 20)
    private String cacheStatus;

    @Column(name = "token_count_input")
    private Integer tokenCountInput;

    @Column(name = "token_count_output")
    private Integer tokenCountOutput;

    @Column(name = "latency_ms")
    private Integer latencyMs;

    @Column(name = "cosine_similarity", precision = 6, scale = 5)
    private BigDecimal cosineSimilarity;

    @Column(name = "trace_id", length = 32)
    private String traceId;

    @Column(name = "http_status_code")
    private Integer httpStatusCode;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;
}
