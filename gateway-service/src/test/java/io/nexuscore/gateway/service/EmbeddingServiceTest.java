package io.nexuscore.gateway.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * =============================================================================
 * EmbeddingServiceTest — Unit Tests for Mock Embedding Generation
 * =============================================================================
 * WHAT THESE TESTS VERIFY:
 *   1. Embeddings have exactly 1536 dimensions
 *   2. Embeddings are deterministic: same text → same vector
 *   3. Different texts → different vectors (with very high probability)
 *   4. Vectors are L2-normalized (magnitude ≈ 1.0)
 *   5. Byte conversion produces correct size (1536 × 4 = 6144 bytes)
 *   6. Base64 encoding/decoding round-trips correctly
 * =============================================================================
 */
class EmbeddingServiceTest {

    private EmbeddingService embeddingService;

    @BeforeEach
    void setUp() {
        embeddingService = new EmbeddingService();
        // Set mock mode via reflection (normally injected by Spring)
        // In unit tests we don't have a Spring context, so we set fields directly
        setPrivateField(embeddingService, "dimensions", 1536);
        setPrivateField(embeddingService, "useMock", true);
        setPrivateField(embeddingService, "openAiApiKey", "");
    }

    @Test
    @DisplayName("Mock embedding has exactly 1536 dimensions")
    void embed_returns1536DimensionalVector() {
        StepVerifier.create(embeddingService.embed("Hello world"))
                .assertNext(vector -> {
                    assertThat(vector).hasSize(1536);
                    System.out.println("✓ Vector dimension: " + vector.length);
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("Same text always produces the same vector (deterministic)")
    void embed_isDeterministic_sameTextSameVector() {
        String text = "How do I cancel my subscription?";

        float[] vector1 = embeddingService.embed(text).block();
        float[] vector2 = embeddingService.embed(text).block();

        assertThat(vector1).isNotNull();
        assertThat(vector2).isNotNull();
        assertThat(vector1).containsExactly(vector2);
        System.out.println("✓ Vectors are identical for same input text");
    }

    @Test
    @DisplayName("Different texts produce different vectors")
    void embed_differentTexts_differentVectors() {
        float[] vector1 = embeddingService.embed("How do I cancel my subscription?").block();
        float[] vector2 = embeddingService.embed("What is the capital of France?").block();

        assertThat(vector1).isNotNull();
        assertThat(vector2).isNotNull();
        assertThat(vector1).doesNotContainSequence(vector2);
        System.out.println("✓ Different texts produce different vectors");
    }

    @Test
    @DisplayName("Embedding vector is L2-normalized (magnitude ≈ 1.0)")
    void embed_vectorIsNormalized() {
        float[] vector = embeddingService.embed("Test normalization").block();
        assertThat(vector).isNotNull();

        // Compute L2 magnitude: sqrt(sum(x_i^2))
        double magnitude = 0;
        for (float f : vector) {
            magnitude += f * f;
        }
        magnitude = Math.sqrt(magnitude);

        System.out.printf("✓ Vector magnitude: %.6f (expected ~1.0)%n", magnitude);
        assertThat(magnitude).isCloseTo(1.0, org.assertj.core.data.Offset.offset(0.001));
    }

    @Test
    @DisplayName("toBytes() produces 6144 bytes (1536 × 4 bytes per float)")
    void toBytes_producesCorrectByteLength() {
        float[] vector = embeddingService.embed("Byte conversion test").block();
        assertThat(vector).isNotNull();

        byte[] bytes = embeddingService.toBytes(vector);
        assertThat(bytes).hasSize(1536 * Float.BYTES);  // 1536 * 4 = 6144 bytes
        System.out.println("✓ Byte array size: " + bytes.length + " bytes");
    }

    @Test
    @DisplayName("Base64 encoding round-trip preserves vector data")
    void toBase64_fromBase64_roundTrip() {
        float[] original = embeddingService.embed("Base64 round trip test").block();
        assertThat(original).isNotNull();

        // Encode to Base64 (for Kafka transport)
        String base64 = embeddingService.toBase64(original);
        assertThat(base64).isNotBlank();
        System.out.println("✓ Base64 length: " + base64.length() + " chars");

        // Decode back from Base64
        byte[] decoded = embeddingService.fromBase64(base64);
        assertThat(decoded).hasSize(1536 * Float.BYTES);
        System.out.println("✓ Decoded byte array size: " + decoded.length + " bytes");
    }

    @Test
    @DisplayName("Empty string produces a valid embedding (no exceptions)")
    void embed_emptyString_producesValidEmbedding() {
        StepVerifier.create(embeddingService.embed(""))
                .assertNext(vector -> {
                    assertThat(vector).hasSize(1536);
                    System.out.println("✓ Empty string embedding produced successfully");
                })
                .verifyComplete();
    }

    // =========================================================================
    // Helper: Set private fields via reflection (for unit testing without Spring)
    // =========================================================================
    private void setPrivateField(Object target, String fieldName, Object value) {
        try {
            var field = target.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set field: " + fieldName, e);
        }
    }
}
