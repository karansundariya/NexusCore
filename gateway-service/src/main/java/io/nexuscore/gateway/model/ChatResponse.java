package io.nexuscore.gateway.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * =============================================================================
 * ChatResponse — OpenAI-Compatible Chat Completions Response DTO
 * =============================================================================
 * WHAT THIS IS:
 *   Response object returned by POST /v1/chat/completions. Mirrors the OpenAI
 *   Chat Completions API response schema for client compatibility.
 *
 * CUSTOM HEADERS ADDED BY NEXUSCORE:
 *   X-Cache-Status: SEMANTIC_HIT | MISS
 *   X-Request-Id: <uuid>
 *   X-Trace-Id: <opentelemetry trace id>
 *   X-Cosine-Similarity: 0.979 (only on SEMANTIC_HIT)
 *   Retry-After: 60 (only on 429 responses)
 *
 * EXAMPLE RESPONSE:
 *   {
 *     "id": "chatcmpl-nexus-abc123",
 *     "object": "chat.completion",
 *     "created": 1699000000,
 *     "model": "gpt-4o",
 *     "choices": [{
 *       "index": 0,
 *       "message": {"role": "assistant", "content": "To cancel your subscription..."},
 *       "finish_reason": "stop"
 *     }],
 *     "usage": {"prompt_tokens": 20, "completion_tokens": 150, "total_tokens": 170}
 *   }
 * =============================================================================
 */
@Data
@Builder
public class ChatResponse {

    /** Unique ID for this completion — prefixed with "chatcmpl-nexus-" */
    private String id;

    /** Always "chat.completion" for non-streaming responses */
    private String object;

    /** Unix timestamp of when the response was created */
    private long created;

    /** The model that generated this response */
    private String model;

    /** List of response choices (always 1 for NexusCore; OpenAI supports n>1) */
    private List<Choice> choices;

    /** Token usage statistics for cost tracking */
    private Usage usage;

    /** NexusCore extension: cache status for observability */
    @JsonProperty("x_cache_status")
    private String cacheStatus;

    /** NexusCore extension: cosine similarity score (null if cache miss) */
    @JsonProperty("x_cosine_similarity")
    private Double cosineSimilarity;

    /**
     * Factory method: create a standard response with a single assistant message.
     */
    public static ChatResponse of(String content, String model, int promptTokens, int completionTokens, String cacheStatus) {
        return ChatResponse.builder()
                .id("chatcmpl-nexus-" + UUID.randomUUID().toString().substring(0, 8))
                .object("chat.completion")
                .created(Instant.now().getEpochSecond())
                .model(model)
                .choices(List.of(Choice.builder()
                        .index(0)
                        .message(new ChatRequest.Message("assistant", content))
                        .finishReason("stop")
                        .build()))
                .usage(Usage.builder()
                        .promptTokens(promptTokens)
                        .completionTokens(completionTokens)
                        .totalTokens(promptTokens + completionTokens)
                        .build())
                .cacheStatus(cacheStatus)
                .build();
    }

    // =========================================================================
    // Nested DTOs
    // =========================================================================

    @Data
    @Builder
    public static class Choice {
        private int index;
        private ChatRequest.Message message;
        @JsonProperty("finish_reason")
        private String finishReason;
    }

    @Data
    @Builder
    public static class Usage {
        @JsonProperty("prompt_tokens")
        private int promptTokens;
        @JsonProperty("completion_tokens")
        private int completionTokens;
        @JsonProperty("total_tokens")
        private int totalTokens;
    }
}
