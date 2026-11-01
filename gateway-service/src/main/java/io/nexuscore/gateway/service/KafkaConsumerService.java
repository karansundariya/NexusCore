package io.nexuscore.gateway.service;

import io.nexuscore.gateway.model.TelemetryEvent;
import io.nexuscore.gateway.repository.AuditLogRepository;
import io.nexuscore.gateway.entity.LlmAuditLog;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.Instant;

/**
 * =============================================================================
 * KafkaConsumerService — Telemetry Event Consumer & Audit Log Writer
 * =============================================================================
 * WHAT THIS SERVICE DOES:
 *   Listens to the Kafka topic `ai.telemetry.events`. When the Python LangGraph
 *   worker finishes processing a prompt, it publishes a TelemetryEvent containing:
 *   - Token counts (input and output)
 *   - Latency in milliseconds
 *   - Model used (including fallback info)
 *   - Cosine similarity score
 *   This service persists each event to the PostgreSQL `llm_audit_logs` table.
 *
 * WHY A SEPARATE CONSUMER FOR TELEMETRY?
 *   Decoupling telemetry persistence from the hot path (request serving) means:
 *   1. Audit log writes never slow down API responses
 *   2. If Postgres is temporarily unavailable, telemetry events accumulate in
 *      Kafka (with its 7-day retention) and can be replayed after recovery
 *   3. The Python worker doesn't need a Postgres connection — it just publishes
 *      to Kafka and lets this consumer handle persistence
 *
 * KAFKA CONSUMER CONFIGURATION:
 *   - consumer-group: nexuscore-audit-consumer
 *   - enable-auto-commit: false (manual ACK after successful Postgres write)
 *   - auto-offset-reset: earliest (replay all events from start if group is new)
 *
 * WHY MANUAL ACKNOWLEDGMENT (enable-auto-commit=false)?
 *   If auto-commit is enabled, Kafka commits the offset as soon as the message
 *   is received, BEFORE we've written it to Postgres. If Postgres write fails,
 *   the offset is already committed — the event is permanently lost.
 *   With manual ACK, we commit the offset ONLY after a successful Postgres write.
 *   This gives us at-least-once delivery semantics for the audit log.
 * =============================================================================
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaConsumerService {

    private final AuditLogRepository auditLogRepository;

    /**
     * Kafka listener for telemetry events from the Python LangGraph worker.
     *
     * <p>This method is called once per Kafka message. The @KafkaListener annotation
     * configures it to consume from the `ai.telemetry.events` topic.
     *
     * <p>FLOW:
     *   1. Kafka delivers a TelemetryEvent message
     *   2. We persist it to PostgreSQL via JPA
     *   3. On success: manually ACK the Kafka offset
     *   4. On failure: log error, do NOT ACK (Kafka will redeliver)
     *
     * @param event          The deserialized TelemetryEvent from Kafka
     * @param acknowledgment Kafka manual acknowledgment handle
     */
    @KafkaListener(
            topics = "${nexuscore.kafka.topics.telemetry-events:ai.telemetry.events}",
            groupId = "${spring.kafka.consumer.group-id:nexuscore-audit-consumer}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    @Async  // Run in a separate thread to avoid blocking the Kafka listener thread
    public void consumeTelemetryEvent(TelemetryEvent event, Acknowledgment acknowledgment) {
        log.debug("📥 Received telemetry event: requestId={}, model={}, cacheStatus={}",
                event.getRequestId(), event.getModelUsed(), event.getCacheStatus());

        try {
            // Persist the telemetry event to the audit log
            persistAuditLog(event);

            // Manual ACK: only commit offset after successful Postgres write
            acknowledgment.acknowledge();

            log.info("✓ Telemetry event persisted and ACK'd: requestId={}, latency={}ms, tokens={}/{}",
                    event.getRequestId(),
                    event.getLatencyMs(),
                    event.getTokenCountInput(),
                    event.getTokenCountOutput());

        } catch (Exception e) {
            // DO NOT acknowledge on failure — Kafka will redeliver the message
            // after the consumer's session timeout expires
            log.error("✗ Failed to persist telemetry event: requestId={}, error={}. " +
                    "NOT ACK-ing — Kafka will redeliver.",
                    event.getRequestId(), e.getMessage(), e);
            // Note: In production, after N retries, configure a DLQ (Dead Letter Queue)
            // to capture persistently failing events for manual inspection
        }
    }

    /**
     * Converts a TelemetryEvent to a JPA entity and persists it to PostgreSQL.
     *
     * @param event The telemetry event from Kafka
     */
    private void persistAuditLog(TelemetryEvent event) {
        LlmAuditLog auditLog = LlmAuditLog.builder()
                .requestId(event.getRequestId())
                .apiKeyHash(event.getApiKeyHash())
                .promptText(event.getPromptText() != null && event.getPromptText().length() > 10000
                        ? event.getPromptText().substring(0, 10000)  // Truncate very long prompts
                        : event.getPromptText())
                .modelUsed(event.getModelUsed())
                .cacheStatus(event.getCacheStatus())
                .tokenCountInput(event.getTokenCountInput())
                .tokenCountOutput(event.getTokenCountOutput())
                .latencyMs(event.getLatencyMs())
                .cosineSimilarity(event.getCosineSimilarity() != null
                        ? java.math.BigDecimal.valueOf(event.getCosineSimilarity())
                        : null)
                .traceId(event.getTraceId())
                .httpStatusCode(event.getHttpStatusCode())
                .createdAt(Instant.ofEpochMilli(event.getCreatedAt())
                        .atOffset(java.time.ZoneOffset.UTC))
                .build();

        auditLogRepository.save(auditLog);
        log.debug("Saved audit log: id={}", auditLog.getId());
    }
}
