package io.nexuscore.gateway.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.util.List;
import java.util.Map;

/**
 * =============================================================================
 * ChatRequest — OpenAI-Compatible Chat Completions Request DTO
 * =============================================================================
 * WHAT THIS IS:
 *   Data Transfer Object (DTO) representing the JSON body of a chat completions
 *   request. Designed to be wire-compatible with OpenAI's Chat Completions API
 *   (POST /v1/chat/completions) so that existing OpenAI clients work with
 *   NexusCore with zero code changes.
 *
 * WHY OPENAI-COMPATIBLE?
 *   OpenAI's API has become the de-facto standard for LLM APIs. LiteLLM,
 *   LangChain, and most enterprise tooling support it natively. By accepting
 *   the same JSON schema, NexusCore acts as a transparent proxy/gateway —
 *   teams can point their existing OpenAI clients at NexusCore and gain:
 *   semantic caching, rate limiting, observability, and multi-model routing
 *   without any application changes.
 *
 * EXAMPLE REQUEST BODY:
 *   {
 *     "model": "gpt-4o",
 *     "messages": [
 *       {"role": "system", "content": "You are a helpful assistant."},
 *       {"role": "user", "content": "How do I cancel my subscription?"}
 *     ],
 *     "temperature": 0.7,
 *     "stream": false,
 *     "max_tokens": 1024
 *   }
 * =============================================================================
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ChatRequest {

    /**
     * The AI model to use (e.g., "gpt-4o", "claude-3-5-sonnet-20241022").
     * NexusCore's Python worker uses LiteLLM to route this to the actual provider.
     */
    @NotBlank(message = "model field is required")
    private String model;

    /**
     * The conversation messages. Must contain at least one message.
     * Typically includes a system message and the user's question.
     */
    @NotEmpty(message = "messages array must not be empty")
    @Valid
    private List<Message> messages;

    /**
     * Sampling temperature: 0.0 = deterministic, 2.0 = most random.
     * Higher temperature produces more creative but less predictable output.
     * Defaults to 0.7 if not specified.
     */
    @Builder.Default
    private Double temperature = 0.7;

    /**
     * Maximum tokens to generate in the response.
     * Limits cost and response length. Null means model default.
     */
    @JsonProperty("max_tokens")
    private Integer maxTokens;

    /**
     * Whether to stream the response as Server-Sent Events.
     * When true: response is a stream of data: chunks
     * When false: response is a single JSON object
     */
    @Builder.Default
    private Boolean stream = false;

    /**
     * Optional request ID for idempotency and tracing.
     * If not provided, the Gateway generates one (UUID v4).
     */
    @JsonProperty("request_id")
    private String requestId;

    /**
     * Additional model-specific parameters (e.g., top_p, frequency_penalty).
     * Passed through to LiteLLM verbatim.
     */
    @JsonProperty("extra_params")
    private Map<String, Object> extraParams;

    // =========================================================================
    // Nested DTO: Message
    // =========================================================================

    /**
     * Represents a single message in the conversation history.
     * Mirrors OpenAI's message object schema exactly.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class Message {

        /**
         * The role of the message author.
         * Allowed values: "system", "user", "assistant", "tool"
         */
        @NotBlank(message = "message role is required")
        private String role;

        /**
         * The text content of the message.
         * For the semantic cache, we extract the last "user" message as the
         * prompt to embed and search for in Redis VSS.
         */
        @NotNull(message = "message content is required")
        private String content;
    }

    /**
     * Extracts the last user message content from the messages array.
     * This is the text that will be embedded for semantic cache lookup.
     *
     * <p>WHY the last user message?
     *   In a multi-turn conversation, the most recent user message carries
     *   the current intent. We embed this for cache matching, not the full
     *   conversation history (which would rarely match exactly).
     *
     * @return The content of the last user-role message, or empty string if none found
     */
    public String extractLastUserMessage() {
        if (messages == null || messages.isEmpty()) {
            return "";
        }
        // Iterate in reverse to find the last user message efficiently
        for (int i = messages.size() - 1; i >= 0; i--) {
            Message msg = messages.get(i);
            if ("user".equals(msg.getRole()) && msg.getContent() != null) {
                return msg.getContent();
            }
        }
        return "";
    }
}
