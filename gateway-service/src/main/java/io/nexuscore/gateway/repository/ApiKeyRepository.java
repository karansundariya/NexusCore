package io.nexuscore.gateway.repository;

import io.nexuscore.gateway.entity.ApiKey;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

/**
 * =============================================================================
 * ApiKeyRepository — Spring Data JPA Repository for api_keys
 * =============================================================================
 * Used by RateLimitFilter to validate API keys from the Authorization header.
 * The lookup is by key_hash (SHA-256 of the raw key), never by raw key value.
 * =============================================================================
 */
@Repository
public interface ApiKeyRepository extends JpaRepository<ApiKey, UUID> {

    /**
     * Find an API key by its SHA-256 hash.
     * This is the primary lookup path used during request authentication.
     *
     * @param keyHash SHA-256 hex hash of the raw API key
     * @return Optional<ApiKey> — present if a matching active key exists
     */
    Optional<ApiKey> findByKeyHashAndIsActiveTrue(String keyHash);
}
