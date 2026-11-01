package io.nexuscore.gateway.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * =============================================================================
 * SemanticCacheService — Redis Vector Similarity Search Cache
 * =============================================================================
 * WHAT THIS SERVICE DOES:
 *   Implements the core semantic caching logic using Redis Stack's Vector
 *   Similarity Search (VSS). On every cache-miss request, it:
 *     1. Receives a query vector (1536-dim float[])
 *     2. Converts it to raw bytes (FLOAT32 little-endian) for Redis
 *     3. Executes FT.SEARCH with KNN k=1 to find the closest cached prompt
 *     4. Computes cosine SIMILARITY from the returned cosine DISTANCE
 *     5. Returns a CacheResult indicating HIT (≥0.96) or MISS (<0.96)
 *
 *   On a cache miss (after the LLM generates a response), it:
 *     6. Stores the new prompt+response as a Redis HASH with 24h TTL
 *
 * THE COSINE SIMILARITY MATH:
 *   Redis Stack returns the COSINE DISTANCE (range 0 to 2, where 0 = identical).
 *   We convert: Similarity = 1 - (CosineDistance / 2)
 *   Wait — more precisely:
 *     CosineDistance = 1 - CosineSimilarity  (Redis uses this definition)
 *   So: Similarity = 1 - CosineDistance
 *   At threshold 0.96: CosineDistance must be ≤ 0.04
 *
 * HNSW QUERY UNDER LOAD:
 *   The FT.SEARCH KNN query traverses the HNSW graph from the top layer down.
 *   With 100K cached embeddings, this takes ~1-3ms including Redis network RTT.
 *   The Redis HNSW index keeps the graph in RAM, ensuring consistent latency
 *   even under high read concurrency (many simultaneous FT.SEARCH calls).
 *
 * REACTIVE DESIGN:
 *   All methods return Mono<T>. The Redis execute() calls use Lettuce's
 *   async driver, so no thread is blocked waiting for Redis responses.
 *   This service runs entirely on the Netty event loop + Lettuce I/O threads.
 * =============================================================================
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class SemanticCacheService {

    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final EmbeddingService embeddingService;

    @Value("${nexuscore.semantic-cache.similarity-threshold:0.96}")
    private double similarityThreshold;

    @Value("${nexuscore.semantic-cache.index-name:idx:prompt_vss}")
    private String indexName;

    @Value("${nexuscore.semantic-cache.ttl-seconds:86400}")
    private long ttlSeconds;

    // =========================================================================
    // Cache Lookup: Find the nearest cached embedding
    // =========================================================================

    /**
     * Performs a semantic similarity search for the given prompt vector.
     *
     * <p>ALGORITHM:
     *   1. Convert float[] → byte[] (FLOAT32 little-endian format for Redis)
     *   2. Execute FT.SEARCH via Redis execute() with the KNN query
     *   3. Parse the result to extract the response text and distance score
     *   4. Compute: similarity = 1 - distance
     *   5. If similarity ≥ threshold: return CacheHit with cached response
     *   6. Else: return CacheMiss
     *
     * <p>The FT.SEARCH command:
     *   FT.SEARCH idx:prompt_vss "*=>[KNN 1 @vector $query_vec AS score]"
     *     RETURN 3 prompt_text response_text score
     *     PARAMS 2 query_vec <6144_bytes>
     *     SORTBY score
     *     DIALECT 2
     *
     *   Explanation:
     *   - "*=>[KNN 1 @vector $query_vec AS score]": KNN filter expression.
     *     Find the 1 nearest neighbor in the @vector field for the $query_vec parameter.
     *     The distance is aliased as 'score'.
     *   - RETURN 3 ... : return only these 3 fields (not the raw vector bytes!)
     *   - PARAMS 2 query_vec <bytes>: supply the query vector as a parameter
     *   - SORTBY score: sort results by distance (nearest first)
     *   - DIALECT 2: required for vector search syntax (introduced in Redis 7.x)
     *
     * @param queryVector The 1536-dim float[] embedding of the incoming prompt
     * @return Mono<CacheResult> — either a HIT with cached response or a MISS
     */
    public Mono<CacheResult> lookup(float[] queryVector) {
        // Convert the float vector to raw bytes for Redis
        byte[] vectorBytes = embeddingService.toBytes(queryVector);

        log.debug("Executing Redis VSS KNN query (vector dim={})", queryVector.length);
        long startMs = System.currentTimeMillis();

        // Build the FT.SEARCH command arguments
        // We use reactive execute with a raw Redis command since ReactiveRedisTemplate
        // doesn't have a built-in FT.SEARCH binding — we use the low-level execute API.
        return redisTemplate.execute(connection -> {
                    // Build the FT.SEARCH command as raw bytes
                    // HNSW KNN query: find 1 nearest neighbor to our query vector
                    byte[][] cmd = new byte[][] {
                            "FT.SEARCH".getBytes(StandardCharsets.UTF_8),
                            indexName.getBytes(StandardCharsets.UTF_8),
                            "*=>[KNN 1 @vector $query_vec AS score]".getBytes(StandardCharsets.UTF_8),
                            "RETURN".getBytes(StandardCharsets.UTF_8),
                            "3".getBytes(StandardCharsets.UTF_8),
                            "response_text".getBytes(StandardCharsets.UTF_8),
                            "prompt_text".getBytes(StandardCharsets.UTF_8),
                            "score".getBytes(StandardCharsets.UTF_8),
                            "PARAMS".getBytes(StandardCharsets.UTF_8),
                            "2".getBytes(StandardCharsets.UTF_8),
                            "query_vec".getBytes(StandardCharsets.UTF_8),
                            vectorBytes,
                            "SORTBY".getBytes(StandardCharsets.UTF_8),
                            "score".getBytes(StandardCharsets.UTF_8),
                            "DIALECT".getBytes(StandardCharsets.UTF_8),
                            "2".getBytes(StandardCharsets.UTF_8)
                    };
                    return connection.execute("FT.SEARCH", cmd);
                })
                .collectList()
                .map(results -> {
                    long queryMs = System.currentTimeMillis() - startMs;
                    return parseSearchResults(results, queryMs);
                })
                .doOnError(e -> log.error("Redis VSS lookup error: {}", e.getMessage()))
                .onErrorReturn(CacheResult.miss());  // Fail open: treat errors as cache miss
    }

    /**
     * Stores a prompt+response pair in the Redis semantic cache.
     *
     * <p>Writes a Redis HASH at key "prompt:<uuid>" with fields:
     *   - vector: raw FLOAT32 bytes of the embedding (6144 bytes)
     *   - prompt_text: the original prompt string
     *   - response_text: the LLM's response
     *   - model_used: which model generated this response
     *   - created_at: Unix epoch timestamp
     *
     * <p>Sets a TTL of 24 hours (86400 seconds). After TTL expires, Redis
     * automatically deletes the key. This prevents stale responses from
     * being served indefinitely. The 24h TTL balances freshness vs hit rate:
     * - Longer TTL: more hits, but responses may become outdated
     * - Shorter TTL: fresher responses, but lower hit rate
     * - 24h: matches typical business day cycle; most support answers remain valid
     *
     * @param promptText   The original prompt text
     * @param responseText The LLM response to cache
     * @param model        The model that generated the response
     * @param vector       The prompt's embedding vector
     * @return Mono<String> emitting the Redis key of the stored entry
     */
    public Mono<String> store(String promptText, String responseText, String model, float[] vector) {
        String key = "prompt:" + UUID.randomUUID();
        byte[] vectorBytes = embeddingService.toBytes(vector);
        long now = Instant.now().getEpochSecond();

        log.debug("Storing cache entry: key={}, model={}, prompt='{}'",
                key, model, promptText.length() > 50 ? promptText.substring(0, 50) + "..." : promptText);

        // Build the HASH map. Note: vector bytes stored as raw binary in the HASH field.
        // Spring's StringRedisSerializer handles String fields; we use the low-level
        // connection for the binary vector field.
        Map<String, String> hashFields = Map.of(
                "prompt_text", promptText,
                "response_text", responseText,
                "model_used", model,
                "created_at", String.valueOf(now)
        );

        // Step 1: Store the text fields via ReactiveHashOperations
        return redisTemplate.opsForHash()
                .putAll(key, hashFields)
                .then(
                    // Step 2: Store the binary vector field via raw command
                    // We can't use the string template for binary data — use execute()
                    redisTemplate.execute(conn -> {
                        return conn.hashCommands().hSet(
                                key.getBytes(StandardCharsets.UTF_8),
                                "vector".getBytes(StandardCharsets.UTF_8),
                                vectorBytes
                        );
                    }).then()
                )
                .then(
                    // Step 3: Set 24-hour TTL on the key
                    redisTemplate.expire(key, Duration.ofSeconds(ttlSeconds))
                )
                .thenReturn(key)
                .doOnSuccess(k -> log.info("✓ Cache stored: key={}, TTL={}s", k, ttlSeconds))
                .doOnError(e -> log.error("Failed to store cache entry: {}", e.getMessage()));
    }

    // =========================================================================
    // Private: Parse FT.SEARCH Results
    // =========================================================================

    /**
     * Parses the raw FT.SEARCH response list into a CacheResult.
     *
     * <p>Redis FT.SEARCH returns results in this flat list format:
     *   [total_count, key1, [field1, value1, field2, value2, ...], key2, [...], ...]
     *   Index 0: Long (total number of matching documents)
     *   Index 1: String (key of first result, e.g., "prompt:abc-123")
     *   Index 2: List (field-value pairs for the first result)
     *
     * @param results Raw result list from FT.SEARCH
     * @param queryMs Time taken for the query in milliseconds
     * @return CacheResult (HIT or MISS)
     */
    @SuppressWarnings("unchecked")
    private CacheResult parseSearchResults(java.util.List<Object> results, long queryMs) {
        // FT.SEARCH result[0] is the total count of matching documents
        if (results == null || results.isEmpty()) {
            log.debug("Redis VSS: no results returned (query took {}ms)", queryMs);
            return CacheResult.miss();
        }

        Object countObj = results.get(0);
        long count = countObj instanceof Long l ? l : 0L;

        if (count == 0) {
            log.info("Cache MISS: no cached entries found (query took {}ms)", queryMs);
            return CacheResult.miss();
        }

        // Parse the first (nearest) result
        // results[2] is the flat field-value list for the first document
        if (results.size() < 3) {
            log.warn("Unexpected FT.SEARCH result format: size={}", results.size());
            return CacheResult.miss();
        }

        java.util.List<Object> fieldValues;
        Object fieldsObj = results.get(2);
        if (fieldsObj instanceof java.util.List<?> list) {
            fieldValues = (java.util.List<Object>) list;
        } else {
            log.warn("FT.SEARCH field values not a List: {}", fieldsObj);
            return CacheResult.miss();
        }

        // Parse field-value pairs into a map
        String responseText = null;
        String promptText = null;
        double score = Double.MAX_VALUE;

        for (int i = 0; i + 1 < fieldValues.size(); i += 2) {
            String fieldName = fieldValues.get(i) != null ? fieldValues.get(i).toString() : "";
            Object fieldVal = fieldValues.get(i + 1);
            String value = fieldVal != null ? fieldVal.toString() : "";

            switch (fieldName) {
                case "response_text" -> responseText = value;
                case "prompt_text"   -> promptText = value;
                case "score"         -> {
                    try {
                        // 'score' is the cosine DISTANCE (range [0, 2])
                        // 0 = identical, 1 = orthogonal, 2 = opposite
                        score = Double.parseDouble(value);
                    } catch (NumberFormatException e) {
                        log.warn("Could not parse score value: '{}'", value);
                    }
                }
            }
        }

        // Convert cosine distance to cosine similarity
        // Redis COSINE metric returns: distance = 1 - cosine_similarity
        // So: cosine_similarity = 1 - distance
        double similarity = 1.0 - score;

        log.info("🔍 VSS Result: Cosine Distance={:.4f} → Similarity={:.1f}% | Threshold={}% | {} ({}ms)",
                score, similarity * 100, similarityThreshold * 100,
                similarity >= similarityThreshold ? "CACHE HIT ✓" : "CACHE MISS ✗",
                queryMs);

        if (similarity >= similarityThreshold) {
            log.info("✅ SEMANTIC_HIT: Returning cached response (similarity={}%)",
                    String.format("%.1f", similarity * 100));
            return CacheResult.hit(responseText != null ? responseText : "", similarity);
        } else {
            log.info("❌ CACHE MISS: Similarity {}% below threshold {}%",
                    String.format("%.1f", similarity * 100),
                    String.format("%.0f", similarityThreshold * 100));
            return CacheResult.miss();
        }
    }

    // =========================================================================
    // Inner Class: CacheResult
    // =========================================================================

    /**
     * Represents the result of a semantic cache lookup.
     * Either a HIT (with cached response and similarity score) or a MISS.
     */
    public record CacheResult(
            boolean hit,
            Optional<String> cachedResponse,
            double similarity
    ) {
        /** Factory: create a cache hit result */
        public static CacheResult hit(String response, double similarity) {
            return new CacheResult(true, Optional.of(response), similarity);
        }

        /** Factory: create a cache miss result */
        public static CacheResult miss() {
            return new CacheResult(false, Optional.empty(), 0.0);
        }
    }
}
