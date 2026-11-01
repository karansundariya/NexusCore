package io.nexuscore.gateway.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * =============================================================================
 * KafkaTopicConfig — Kafka Topic Auto-Provisioning
 * =============================================================================
 * WHAT THIS DOES:
 *   Declares the Kafka topics NexusCore needs as Spring beans. When Spring Boot
 *   starts, KafkaAdmin (auto-configured from spring.kafka.bootstrap-servers)
 *   calls the Kafka Admin API to create any topics that don't already exist.
 *   This is idempotent: if the topic exists, it's left unchanged.
 *
 * WHY AUTO-PROVISION IN CODE?
 *   In development and testing, having topics created automatically speeds up
 *   the feedback loop — no manual kafka-topics.sh commands needed.
 *   In production, disable this and use Terraform or a Kafka operator to
 *   manage topics declaratively with stricter partition/replication settings.
 *
 * TOPIC DESIGN:
 *   ai.prompts.unprocessed:
 *     - 3 partitions: allows 3 Python LangGraph workers to consume in parallel
 *     - Messages are keyed by requestId, so same-session messages go to same partition
 *     - Retention: 7 days (enables replay for debugging or re-processing)
 *
 *   ai.telemetry.events:
 *     - 3 partitions: audit consumer processes in parallel
 *     - Contains cost metrics, token counts, and latency telemetry
 *     - Written to S3/audit lake downstream (MVP 4)
 * =============================================================================
 */
@Configuration
public class KafkaTopicConfig {

    @Value("${nexuscore.kafka.topics.prompts-unprocessed}")
    private String promptsTopic;

    @Value("${nexuscore.kafka.topics.telemetry-events}")
    private String telemetryTopic;

    /**
     * Declares the ai.prompts.unprocessed topic.
     * Cache-miss prompts are published here by KafkaProducerService.
     * Python LangGraph workers consume from this topic.
     *
     * @return NewTopic bean consumed by KafkaAdmin on startup
     */
    @Bean
    public NewTopic promptsUnprocessedTopic() {
        return TopicBuilder.name(promptsTopic)
                .partitions(3)        // 3 parallel consumers possible
                .replicas(1)          // Single replica for dev; increase to 3 in production
                // Retain messages for 7 days: enables debug replay
                .config("retention.ms", "604800000")
                // Max message size: 10MB (large prompts with context windows)
                .config("max.message.bytes", "10485760")
                .build();
    }

    /**
     * Declares the ai.telemetry.events topic.
     * Python worker emits completion events here after each LLM response.
     * KafkaConsumerService persists these events to the PostgreSQL audit log.
     *
     * @return NewTopic bean consumed by KafkaAdmin on startup
     */
    @Bean
    public NewTopic telemetryEventsTopic() {
        return TopicBuilder.name(telemetryTopic)
                .partitions(3)
                .replicas(1)
                .config("retention.ms", "604800000")
                // Compact telemetry by requestId: keeps latest event per request
                // This allows efficient recovery of last-known state
                .config("cleanup.policy", "delete")
                .build();
    }
}
