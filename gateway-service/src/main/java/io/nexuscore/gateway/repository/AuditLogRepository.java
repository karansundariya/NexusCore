package io.nexuscore.gateway.repository;

import io.nexuscore.gateway.entity.LlmAuditLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

/**
 * =============================================================================
 * AuditLogRepository — Spring Data JPA Repository for llm_audit_logs
 * =============================================================================
 * Spring Data JPA auto-generates SQL for all standard CRUD operations.
 * We add custom query methods for analytics and cost-tracking dashboards.
 * =============================================================================
 */
@Repository
public interface AuditLogRepository extends JpaRepository<LlmAuditLog, UUID> {

    /** Find all audit logs for a specific request (for debugging) */
    List<LlmAuditLog> findByRequestId(String requestId);

    /** Find all logs for a specific model (for cost comparison) */
    List<LlmAuditLog> findByModelUsed(String modelUsed);

    /** Count cache hits vs misses for a given time window */
    @Query("SELECT COUNT(l) FROM LlmAuditLog l WHERE l.cacheStatus = :status")
    long countByCacheStatus(String status);

    /** Calculate total estimated cost: sum of all token usage */
    @Query("SELECT SUM(l.tokenCountInput + l.tokenCountOutput) FROM LlmAuditLog l WHERE l.modelUsed = :model")
    Long totalTokensForModel(String model);
}
