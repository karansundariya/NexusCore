package io.nexuscore.gateway.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * =============================================================================
 * ApiKey — JPA Entity for api_keys Table
 * =============================================================================
 * Maps to the PostgreSQL table created in init_postgres.sql.
 * Used by RateLimitFilter to validate incoming API keys and retrieve their
 * rate limit configuration (rate_limit_rpm).
 * =============================================================================
 */
@Entity
@Table(name = "api_keys")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ApiKey {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "name", nullable = false, length = 255)
    private String name;

    /**
     * SHA-256 hash of the raw API key.
     * This is what the Gateway stores and looks up — never the raw key.
     */
    @Column(name = "key_hash", nullable = false, unique = true, length = 64)
    private String keyHash;

    @Column(name = "rate_limit_rpm", nullable = false)
    private Integer rateLimitRpm;

    @Column(name = "is_active", nullable = false)
    private Boolean isActive;

    @Column(name = "tenant_id")
    private UUID tenantId;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    @Column(name = "last_used_at")
    private OffsetDateTime lastUsedAt;

    @Column(name = "expires_at")
    private OffsetDateTime expiresAt;
}
