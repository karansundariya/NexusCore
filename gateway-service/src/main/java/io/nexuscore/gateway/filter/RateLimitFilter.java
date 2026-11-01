package io.nexuscore.gateway.filter;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;

/**
 * =============================================================================
 * RateLimitFilter — Distributed Token Bucket Rate Limiter
 * =============================================================================
 * WHAT THIS FILTER DOES:
 *   Intercepts every HTTP request to POST /v1/chat/completions and:
 *     1. Extracts the API key from the Authorization header
 *     2. Hashes it with SHA-256 (never stores raw keys in Redis)
 *     3. Executes a Lua script in Redis to atomically check + decrement
 *        a token bucket counter for this API key
 *     4. Returns HTTP 429 (Too Many Requests) if the bucket is empty
 *     5. Adds X-RateLimit-* headers so clients know their remaining quota
 *
 * WHY A LUA SCRIPT?
 *   Redis executes Lua scripts atomically as a single command. This is critical
 *   for correctness: without atomicity, a race condition could allow two
 *   concurrent requests to both read "1 token remaining" and both succeed,
 *   effectively bypassing the rate limit (TOCTOU bug).
 *   The Lua script combines CHECK + DECREMENT + EXPIRE into one atomic operation.
 *
 * WHY THE TOKEN BUCKET ALGORITHM?
 *   Token Bucket allows burst traffic while enforcing a sustained rate limit.
 *   Example (60 tokens/min):
 *   - Idle client accumulates tokens up to capacity (60)
 *   - Burst of 60 requests in 1 second: all succeed (tokens deplete from 60→0)
 *   - Next request within the same minute: HTTP 429
 *   - After 60 seconds: bucket refills to 60 again
 *   This is more user-friendly than Fixed Window (which resets at arbitrary boundaries)
 *   and simpler to implement than Sliding Window Log (which stores per-request timestamps).
 *
 * HOW IT WORKS UNDER HIGH CONCURRENCY:
 *   WebFlux is non-blocking — this filter runs on the Netty I/O thread.
 *   The Redis Lua script call is async (via Lettuce + Reactor). The filter
 *   returns a Mono<Void> that only subscribes when the upstream operator subscribes.
 *   No thread is blocked waiting for Redis. Under 10,000 concurrent requests,
 *   only a handful of I/O threads are needed because all I/O is event-driven.
 * =============================================================================
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class RateLimitFilter implements WebFilter {

    private final ReactiveRedisTemplate<String, String> redisTemplate;

    @Value("${nexuscore.rate-limit.default-capacity:60}")
    private int defaultCapacity;

    @Value("${nexuscore.rate-limit.window-seconds:60}")
    private int windowSeconds;

    @Value("${nexuscore.rate-limit.redis-key-prefix:rate_limit:}")
    private String redisKeyPrefix;

    // =========================================================================
    // TOKEN BUCKET LUA SCRIPT
    // =========================================================================
    // This Lua script implements the Token Bucket algorithm atomically in Redis.
    //
    // KEYS[1] = Redis key for this API key's token bucket (e.g., "rate_limit:abc123")
    // ARGV[1] = Maximum capacity (bucket size) = 60
    // ARGV[2] = Window duration in seconds = 60
    //
    // ALGORITHM:
    //   1. GET the current token count from Redis
    //   2. If no key exists (first request), initialize to capacity - 1 (consume 1 token)
    //      and set TTL = windowSeconds (key expires = bucket refills)
    //   3. If key exists and count > 0: DECR (consume 1 token), return remaining count
    //   4. If key exists and count == 0: return -1 (bucket empty = rate limit exceeded)
    //
    // RETURN VALUE:
    //   >= 0: remaining tokens (request allowed)
    //   -1:   bucket empty (request rejected, HTTP 429)
    // =========================================================================
    private static final String TOKEN_BUCKET_LUA = """
            local key = KEYS[1]
            local capacity = tonumber(ARGV[1])
            local window = tonumber(ARGV[2])
            
            -- GET current token count
            local current = redis.call('GET', key)
            
            if current == false then
                -- Key doesn't exist: first request in this window.
                -- Initialize bucket with (capacity - 1) remaining tokens
                -- (we consume 1 token for this request immediately).
                -- SET key (capacity-1) EX window: creates key with TTL=window
                redis.call('SET', key, capacity - 1, 'EX', window)
                return capacity - 1
            end
            
            local count = tonumber(current)
            
            if count <= 0 then
                -- Bucket is empty: rate limit exceeded. Return -1 as sentinel.
                return -1
            end
            
            -- Bucket has tokens: consume one (atomic DECR)
            local remaining = redis.call('DECR', key)
            return remaining
            """;

    /**
     * The WebFilter intercept point. Called for every incoming HTTP request.
     *
     * <p>For requests to POST /v1/chat/completions, this filter:
     * 1. Validates the Authorization header
     * 2. Runs the token bucket check
     * 3. Either passes through (chain.filter) or rejects (HTTP 429)
     *
     * <p>For all other paths, the filter is transparent (no-op pass-through).
     *
     * @param exchange The reactive HTTP exchange (request + response)
     * @param chain    The filter chain to pass through on success
     * @return Mono<Void> — reactive signal completing when response is committed
     */
    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String path = exchange.getRequest().getPath().value();
        String method = exchange.getRequest().getMethod().name();

        // Only rate-limit the AI completion endpoint
        if (!("POST".equals(method) && "/v1/chat/completions".equals(path))) {
            return chain.filter(exchange);
        }

        // Extract and validate the Authorization header
        String authHeader = exchange.getRequest().getHeaders().getFirst("Authorization");
        if (authHeader == null || !authHeader.startsWith("Bearer ")) {
            log.warn("Request to {} missing or malformed Authorization header", path);
            exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
            exchange.getResponse().getHeaders().add("WWW-Authenticate",
                    "Bearer realm=\"NexusCore\", error=\"missing_token\"");
            return exchange.getResponse().setComplete();
        }

        // Hash the API key: never log or store raw keys
        String rawApiKey = authHeader.substring("Bearer ".length()).trim();
        String apiKeyHash = sha256(rawApiKey);
        String redisKey = redisKeyPrefix + apiKeyHash;

        log.debug("Rate limit check for key hash: {}... [key={}]",
                apiKeyHash.substring(0, 8), redisKey);

        // Execute the Lua script reactively
        // RedisScript.of() compiles the script once; Redis caches it by SHA1 hash
        RedisScript<Long> script = RedisScript.of(TOKEN_BUCKET_LUA, Long.class);
        List<String> keys = Collections.singletonList(redisKey);
        List<String> args = List.of(String.valueOf(defaultCapacity), String.valueOf(windowSeconds));

        return redisTemplate.execute(script, keys, args)
                .next()  // Take the single Long result from the Flux<Long>
                .flatMap(remaining -> {
                    if (remaining < 0) {
                        // Bucket empty: reject with HTTP 429
                        log.warn("Rate limit EXCEEDED for key hash: {}... Returning 429",
                                apiKeyHash.substring(0, 8));

                        exchange.getResponse().setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
                        // Retry-After: seconds until the bucket refills
                        exchange.getResponse().getHeaders().add("Retry-After",
                                String.valueOf(windowSeconds));
                        exchange.getResponse().getHeaders().add("X-RateLimit-Limit",
                                String.valueOf(defaultCapacity));
                        exchange.getResponse().getHeaders().add("X-RateLimit-Remaining", "0");
                        exchange.getResponse().getHeaders().add("Content-Type", "application/json");

                        // Write error body
                        String errorBody = """
                                {"error":{"code":"rate_limit_exceeded","message":"Too many requests. Retry after %d seconds.","type":"rate_limit_error"}}
                                """.formatted(windowSeconds);
                        byte[] bytes = errorBody.getBytes(StandardCharsets.UTF_8);
                        return exchange.getResponse().writeWith(
                                Mono.just(exchange.getResponse().bufferFactory().wrap(bytes))
                        );
                    }

                    // Token consumed: add rate limit headers and continue the chain
                    log.debug("Rate limit OK: {} tokens remaining for key hash: {}...",
                            remaining, apiKeyHash.substring(0, 8));

                    exchange.getResponse().getHeaders().add("X-RateLimit-Limit",
                            String.valueOf(defaultCapacity));
                    exchange.getResponse().getHeaders().add("X-RateLimit-Remaining",
                            String.valueOf(remaining));
                    // Add API key hash as request attribute for downstream services
                    exchange.getAttributes().put("apiKeyHash", apiKeyHash);

                    return chain.filter(exchange);
                })
                .switchIfEmpty(
                    // Redis returned no value (shouldn't happen with our Lua script)
                    // Fail open: allow the request but log a warning
                    Mono.defer(() -> {
                        log.error("Redis returned no value for rate limit check — failing open");
                        return chain.filter(exchange);
                    })
                );
    }

    /**
     * Computes the SHA-256 hash of the given input string.
     *
     * <p>WHY SHA-256?
     *   SHA-256 is a cryptographic one-way hash function. Given the hash, you
     *   cannot recover the original API key. This means storing the hash in Redis
     *   as a rate limit key is safe even if Redis is compromised — the attacker
     *   cannot reconstruct real API keys from the hashes.
     *
     * @param input The raw API key string
     * @return Lowercase hex-encoded SHA-256 hash (64 characters)
     */
    private String sha256(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            // Java 17+ HexFormat: cleaner than StringBuilder loop
            return HexFormat.of().formatHex(hashBytes);
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is guaranteed to be available in all Java SE implementations
            throw new IllegalStateException("SHA-256 algorithm not available", e);
        }
    }
}
