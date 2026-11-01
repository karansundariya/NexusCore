package io.nexuscore.gateway.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Random;

/**
 * =============================================================================
 * EmbeddingService — Text-to-Vector Embedding Generator
 * =============================================================================
 * WHAT THIS SERVICE DOES:
 *   Converts a text string into a 1536-dimensional float vector (embedding).
 *   These vectors encode the semantic meaning of text — semantically similar
 *   texts produce vectors that are close together in vector space (small cosine
 *   distance), even if the exact words differ.
 *
 * TWO MODES OF OPERATION:
 *   1. MOCK MODE (use-mock=true, default for local development):
 *      Generates a deterministic pseudo-embedding from the SHA-256 hash of the
 *      text. This is NOT semantically meaningful — it's just for integration
 *      testing the pipeline without an OpenAI API key. Two different texts will
 *      produce very different vectors (no semantic similarity). Same text always
 *      produces the same vector (deterministic = testable).
 *
 *   2. REAL MODE (use-mock=false, for staging/production):
 *      Calls OpenAI's text-embedding-3-small API (or ada-002) to generate a
 *      semantically meaningful 1536-dimensional embedding. This enables true
 *      semantic cache matching (paraphrase → cache hit).
 *
 * WHY 1536 DIMENSIONS?
 *   OpenAI's text-embedding-3-small and text-embedding-ada-002 produce 1536-dim
 *   vectors. This is the standard dimension for enterprise RAG pipelines.
 *   Smaller dimensions (e.g., 384 from sentence-transformers) have lower recall;
 *   larger dimensions (e.g., 3072 from text-embedding-3-large) increase Redis
 *   memory usage proportionally with diminishing accuracy returns.
 *
 * WHY IS THIS REACTIVE (Mono<float[]>)?
 *   In production mode, this makes an HTTP call to the OpenAI API. Wrapping it
 *   in Mono + subscribeOn(boundedElastic()) ensures the HTTP call runs on a
 *   non-blocking thread pool, keeping the Netty I/O event loop free.
 *
 * VECTOR REPRESENTATION:
 *   float[] — Java array of 1536 IEEE 754 single-precision floats.
 *   Total memory: 1536 × 4 bytes = 6,144 bytes per embedding vector.
 *   For Redis storage: serialized to a byte[] (little-endian FLOAT32 format)
 *   which RedisSearch's HNSW index expects.
 * =============================================================================
 */
@Service
@Slf4j
public class EmbeddingService {

    @Value("${nexuscore.embedding.dimensions:1536}")
    private int dimensions;

    @Value("${nexuscore.embedding.use-mock:true}")
    private boolean useMock;

    @Value("${nexuscore.embedding.openai-api-key:}")
    private String openAiApiKey;

    /**
     * Generates a float vector embedding for the given text.
     *
     * <p>The returned Mono completes with a float[] of length {@code dimensions} (1536).
     * In mock mode, the operation is synchronous (wrapped in Mono.fromCallable).
     * In production mode, it makes an async HTTP call to the OpenAI API.
     *
     * @param text The prompt text to embed (typically the last user message)
     * @return Mono emitting a 1536-dimensional float vector
     */
    public Mono<float[]> embed(String text) {
        if (useMock) {
            // Mock mode: fast deterministic embedding for local testing
            return Mono.fromCallable(() -> generateMockEmbedding(text))
                    // subscribeOn(boundedElastic) runs this on a worker thread,
                    // keeping the Netty event loop thread free (non-blocking)
                    .subscribeOn(Schedulers.boundedElastic());
        } else {
            // Production mode: call OpenAI embedding API
            return generateRealEmbedding(text);
        }
    }

    /**
     * Converts a float[] embedding to a byte[] for Redis storage.
     *
     * <p>Redis Stack's HNSW vector index expects vectors in raw binary format:
     * - Each float is stored as 4 bytes in little-endian byte order
     * - Total size: 1536 × 4 = 6,144 bytes
     *
     * <p>WHY LITTLE-ENDIAN?
     *   Redis Stack (RedisSearch) on x86/x64 systems stores FLOAT32 vectors in
     *   the native CPU byte order (little-endian). Using ByteBuffer.nativeOrder()
     *   is the safest cross-platform approach.
     *
     * @param vector The float[] embedding vector
     * @return Raw bytes suitable for storage in Redis HASH field "vector"
     */
    public byte[] toBytes(float[] vector) {
        // Allocate a ByteBuffer with enough space for all floats
        ByteBuffer buffer = ByteBuffer.allocate(vector.length * Float.BYTES);
        // Little-endian byte order matches x86 CPU native order and Redis Stack expectation
        buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        for (float f : vector) {
            buffer.putFloat(f);
        }
        return buffer.array();
    }

    /**
     * Encodes a float[] embedding to Base64 for Kafka transport.
     *
     * <p>Kafka messages are byte arrays. We Base64-encode the vector bytes for
     * safe JSON embedding in the PromptEvent Kafka message payload.
     *
     * @param vector The float[] embedding vector
     * @return Base64-encoded string representation of the raw vector bytes
     */
    public String toBase64(float[] vector) {
        return Base64.getEncoder().encodeToString(toBytes(vector));
    }

    /**
     * Decodes a Base64-encoded vector string back to a byte[].
     * Used by the Python cache_writer (transmitted via Kafka) and in tests.
     *
     * @param base64 Base64-encoded vector string
     * @return Raw byte array of the vector
     */
    public byte[] fromBase64(String base64) {
        return Base64.getDecoder().decode(base64);
    }

    // =========================================================================
    // PRIVATE: Mock Embedding Generator
    // =========================================================================

    /**
     * Generates a deterministic 1536-dimensional pseudo-embedding from text.
     *
     * <p>ALGORITHM:
     *   1. Compute SHA-256 hash of the input text (32 bytes)
     *   2. Use the hash bytes as a seed to a seeded Random number generator
     *   3. Generate 1536 random floats in range [-1, +1]
     *   4. L2-normalize the vector (unit length, matching cosine semantics)
     *
     * <p>DETERMINISM GUARANTEE:
     *   The same input text ALWAYS produces the same output vector.
     *   Different texts produce completely different vectors (no semantic meaning).
     *   This is sufficient for testing the cache pipeline but not for production
     *   semantic similarity detection.
     *
     * @param text Input text to embed
     * @return 1536-dimensional float vector (L2-normalized)
     */
    private float[] generateMockEmbedding(String text) {
        log.debug("Generating MOCK embedding for text: '{}'",
                text.length() > 50 ? text.substring(0, 50) + "..." : text);

        // Step 1: Hash the text to derive a deterministic seed
        byte[] hashBytes;
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            hashBytes = digest.digest(text.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }

        // Step 2: Convert first 8 bytes of hash to a long seed
        long seed = 0;
        for (int i = 0; i < 8; i++) {
            seed = (seed << 8) | (hashBytes[i] & 0xFF);
        }

        // Step 3: Generate deterministic random floats from the seeded RNG
        Random rng = new Random(seed);
        float[] vector = new float[dimensions];
        for (int i = 0; i < dimensions; i++) {
            // nextGaussian() produces normally-distributed values — better than uniform
            // for simulating realistic embedding distributions
            vector[i] = (float) rng.nextGaussian();
        }

        // Step 4: L2-normalize to unit length (||v|| = 1)
        // WHY normalize?
        //   Cosine similarity = dot product of unit vectors.
        //   If vectors are not unit length, cosine distance math in Redis still works,
        //   but normalizing ahead of time makes dot products == cosine similarity,
        //   which is a useful property for validation and testing.
        float magnitude = 0f;
        for (float f : vector) {
            magnitude += f * f;
        }
        magnitude = (float) Math.sqrt(magnitude);
        for (int i = 0; i < dimensions; i++) {
            vector[i] /= magnitude;
        }

        log.debug("Mock embedding generated: {} dimensions, magnitude=1.0", dimensions);
        return vector;
    }

    // =========================================================================
    // PRIVATE: Real OpenAI Embedding API Call
    // =========================================================================

    /**
     * Calls the OpenAI text-embedding-3-small API to generate a real semantic embedding.
     *
     * <p>MODEL CHOICE: text-embedding-3-small
     *   - Dimensions: 1536 (same as ada-002)
     *   - Cost: \$0.02 per 1M tokens (~1/5th the cost of ada-002)
     *   - Performance: Higher MTEB benchmark scores than ada-002
     *   - Speed: ~100ms latency on OpenAI infrastructure
     *
     * <p>This method is reactive: it uses Spring WebClient for async HTTP,
     * returning a Mono<float[]> that completes when the API responds.
     *
     * @param text Input text to embed
     * @return Mono emitting the 1536-dim embedding from OpenAI
     */
    private Mono<float[]> generateRealEmbedding(String text) {
        // Use Spring WebClient for reactive HTTP (non-blocking)
        return org.springframework.web.reactive.function.client.WebClient.builder()
                .baseUrl("https://api.openai.com")
                .defaultHeader("Authorization", "Bearer " + openAiApiKey)
                .defaultHeader("Content-Type", "application/json")
                .build()
                .post()
                .uri("/v1/embeddings")
                .bodyValue("""
                        {"model": "text-embedding-3-small", "input": %s}
                        """.formatted(escapeJson(text)))
                .retrieve()
                .bodyToMono(OpenAiEmbeddingResponse.class)
                .map(response -> {
                    // Extract the embedding float array from the API response
                    double[] doubles = response.data().get(0).embedding();
                    float[] floats = new float[doubles.length];
                    for (int i = 0; i < doubles.length; i++) {
                        floats[i] = (float) doubles[i];
                    }
                    log.debug("Real OpenAI embedding generated: {} dimensions", floats.length);
                    return floats;
                })
                .doOnError(e -> log.error("OpenAI embedding API error: {}", e.getMessage()));
    }

    /** Escapes a string for safe inclusion in a JSON string literal */
    private String escapeJson(String text) {
        return "\"" + text.replace("\\", "\\\\")
                          .replace("\"", "\\\"")
                          .replace("\n", "\\n")
                          .replace("\r", "\\r")
                          .replace("\t", "\\t") + "\"";
    }

    // OpenAI API response records (used for JSON deserialization)
    private record OpenAiEmbeddingResponse(java.util.List<EmbeddingData> data) {}
    private record EmbeddingData(double[] embedding) {}
}
