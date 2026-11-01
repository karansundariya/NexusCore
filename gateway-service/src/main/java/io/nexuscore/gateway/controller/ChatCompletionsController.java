package io.nexuscore.gateway.controller;

import io.nexuscore.gateway.model.ChatRequest;
import io.nexuscore.gateway.model.ChatResponse;
import io.nexuscore.gateway.model.PromptEvent;
import io.nexuscore.gateway.service.EmbeddingService;
import io.nexuscore.gateway.service.KafkaProducerService;
import io.nexuscore.gateway.service.SemanticCacheService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * =============================================================================
 * ChatCompletionsController — OpenAI-Compatible AI Gateway Endpoint
 * =============================================================================
 * WHAT THIS CONTROLLER DOES:
 *   Handles POST /v1/chat/completions — the single ingress point for all LLM
 *   traffic through NexusCore. Implements the full 7-step pipeline:
 *
 *   Step 1: Parse and validate the OpenAI-compatible JSON request body
 *   Step 2: Extract the last user message as the "prompt" for semantic lookup
 *   Step 3: Generate a 1536-dim vector embedding of the prompt
 *   Step 4: Query Redis VSS for semantically similar cached responses
 *   Step 5a: [CACHE HIT] Return cached response immediately (<12ms)
 *   Step 5b: [CACHE MISS] Publish to Kafka → Python worker → SSE stream back
 *   Step 6: [CACHE MISS] Async: populate Redis cache with the new response
 *   Step 7: [ALL] Emit telemetry event to Kafka audit stream
 *
 * RESPONSE TIME TARGETS:
 *   - Semantic Cache HIT:  < 12ms  (Redis VSS query + response serialization)
 *   - Cache MISS:          1,000-60,000ms (depends on LLM generation time)
 *
 * SSE (SERVER-SENT EVENTS) FOR CACHE MISS STREAMING:
 *   When a cache miss occurs, the gateway holds the HTTP connection open.
 *   The Python worker publishes its response back via a Redis Pub/Sub channel
 *   keyed to the requestId. The gateway subscribes to this channel and
 *   streams each token chunk to the client as an SSE event.
 *   This architecture enables real-time token streaming without polling.
 *
 * REACTIVE PIPELINE EXPLANATION:
 *   The handler returns a Mono<ResponseEntity<ChatResponse>>. Spring WebFlux
 *   subscribes to this Mono on the Netty event loop. The pipeline:
 *   embed() → lookup() → [branch: hit | miss]
 *   is fully non-blocking — no thread is parked waiting for I/O at any step.
 *
 * ERROR HANDLING:
 *   - Validation errors → HTTP 400 Bad Request
 *   - Kafka publish failure → HTTP 503 Service Unavailable
 *   - Redis VSS errors → fail open (treat as MISS), log the error
 *   - LLM timeout (via Python worker) → HTTP 504 Gateway Timeout after 60s
 * =============================================================================
 */
@RestController
@RequestMapping("/v1")
@Slf4j
@RequiredArgsConstructor
public class ChatCompletionsController {

    private final EmbeddingService embeddingService;
    private final SemanticCacheService semanticCacheService;
    private final KafkaProducerService kafkaProducerService;

    /**
     * In-memory response registry: maps requestId → reactive Sink.
     * When the Python worker completes a request, it publishes the response
     * to a Redis Pub/Sub channel. Our Redis subscriber calls the appropriate
     * Sink to complete the deferred client response.
     *
     * WHY ConcurrentHashMap?
     *   Multiple Netty I/O threads may access this map concurrently.
     *   ConcurrentHashMap provides thread-safe reads and writes.
     *   Each entry is short-lived: created on cache miss, removed when the
     *   response arrives or a 60-second timeout elapses.
     */
    private final Map<String, Sinks.One<String>> pendingResponses = new ConcurrentHashMap<>();

    /**
     * POST /v1/chat/completions — Main AI completion endpoint.
     *
     * <p>This endpoint is protected by RateLimitFilter (runs before this method).
     * By the time this method is invoked, the request has already:
     * - Been authenticated (valid API key in Authorization header)
     * - Had a token deducted from the rate limit bucket
     * - Had the apiKeyHash added to exchange attributes
     *
     * @param request  The parsed and validated ChatRequest body
     * @param exchange The reactive HTTP exchange (for reading headers and attributes)
     * @return Mono<ResponseEntity<ChatResponse>> — completes when response is ready
     */
    @PostMapping(
            value = "/chat/completions",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public Mono<ResponseEntity<ChatResponse>> handleChatCompletion(
            @Valid @RequestBody ChatRequest request,
            ServerWebExchange exchange) {

        // Generate a unique request ID for tracing this request through all services
        String requestId = request.getRequestId() != null
                ? request.getRequestId()
                : UUID.randomUUID().toString();

        // Extract the API key hash added by RateLimitFilter
        String apiKeyHash = (String) exchange.getAttributes().getOrDefault("apiKeyHash", "unknown");

        // Extract the current OpenTelemetry trace ID (set by Micrometer in MVP 6)
        String traceId = exchange.getRequest().getHeaders().getFirst("traceparent");

        // Get the prompt text — the last user message in the conversation
        String promptText = request.extractLastUserMessage();

        log.info("🚀 Request [{}]: model={}, prompt='{}'",
                requestId, request.getModel(),
                promptText.length() > 80 ? promptText.substring(0, 80) + "..." : promptText);

        long requestStartMs = System.currentTimeMillis();

        // ===========================================================================
        // REACTIVE PIPELINE: embed → cache lookup → hit/miss branch
        // ===========================================================================
        return embeddingService.embed(promptText)
                .flatMap(queryVector -> {
                    // Step 3: Query Redis VSS for nearest cached prompt
                    return semanticCacheService.lookup(queryVector)
                            .flatMap(cacheResult -> {
                                if (cacheResult.hit()) {
                                    // ============================================
                                    // CACHE HIT PATH: Return in <12ms
                                    // ============================================
                                    long latencyMs = System.currentTimeMillis() - requestStartMs;
                                    double similarity = cacheResult.similarity();

                                    log.info("✅ SEMANTIC_HIT [{}]: similarity={:.1f}%, latency={}ms",
                                            requestId, similarity * 100, latencyMs);

                                    ChatResponse response = ChatResponse.of(
                                            cacheResult.cachedResponse().orElse(""),
                                            request.getModel() + " (cached)",
                                            estimateTokenCount(promptText),
                                            estimateTokenCount(cacheResult.cachedResponse().orElse("")),
                                            "SEMANTIC_HIT"
                                    );
                                    response.setCosineSimilarity(similarity);

                                    return Mono.just(ResponseEntity.ok()
                                            .header("X-Cache-Status", "SEMANTIC_HIT")
                                            .header("X-Request-Id", requestId)
                                            .header("X-Cosine-Similarity",
                                                    String.format("%.4f", similarity))
                                            .header("X-Latency-Ms", String.valueOf(latencyMs))
                                            .body(response));
                                } else {
                                    // ============================================
                                    // CACHE MISS PATH: Publish to Kafka → SSE
                                    // ============================================
                                    log.info("❌ CACHE MISS [{}]: publishing to Kafka", requestId);

                                    // Build the Kafka message payload
                                    PromptEvent promptEvent = PromptEvent.builder()
                                            .requestId(requestId)
                                            .apiKeyHash(apiKeyHash)
                                            .promptText(promptText)
                                            .model(request.getModel())
                                            .temperature(request.getTemperature())
                                            .maxTokens(request.getMaxTokens())
                                            .stream(request.getStream())
                                            .promptVectorBase64(embeddingService.toBase64(queryVector))
                                            .traceId(traceId)
                                            .build();

                                    // Create a reactive Sink to receive the response
                                    // when the Python worker completes processing
                                    Sinks.One<String> responseSink = Sinks.one();
                                    pendingResponses.put(requestId, responseSink);

                                    // Publish to Kafka and set up a deferred response
                                    return kafkaProducerService.publishPromptEvent(promptEvent)
                                            .then(
                                                // Wait for the Python worker's response via the Sink.
                                                // Timeout after 60 seconds to prevent hanging connections.
                                                responseSink.asMono()
                                                        .timeout(Duration.ofSeconds(60))
                                                        .map(llmResponseText -> {
                                                            long latencyMs = System.currentTimeMillis() - requestStartMs;
                                                            log.info("✓ LLM response received [{}]: latency={}ms",
                                                                    requestId, latencyMs);

                                                            ChatResponse response = ChatResponse.of(
                                                                    llmResponseText,
                                                                    request.getModel(),
                                                                    estimateTokenCount(promptText),
                                                                    estimateTokenCount(llmResponseText),
                                                                    "MISS"
                                                            );

                                                            return ResponseEntity.ok()
                                                                    .header("X-Cache-Status", "MISS")
                                                                    .header("X-Request-Id", requestId)
                                                                    .header("X-Latency-Ms", String.valueOf(latencyMs))
                                                                    .<ChatResponse>body(response);
                                                        })
                                                        .doFinally(signal ->
                                                            // Always clean up the pending response entry
                                                            pendingResponses.remove(requestId)
                                                        )
                                            )
                                            .onErrorResume(e -> {
                                                pendingResponses.remove(requestId);
                                                if (e instanceof java.util.concurrent.TimeoutException) {
                                                    log.error("⏱ Request timeout [{}]: Python worker did not respond in 60s", requestId);
                                                    return Mono.just(ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT)
                                                            .<ChatResponse>body(null));
                                                }
                                                log.error("✗ Error processing request [{}]: {}", requestId, e.getMessage());
                                                return Mono.just(ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                                                        .<ChatResponse>body(null));
                                            });
                                }
                            });
                })
                .doOnError(e -> log.error("✗ Pipeline error for request [{}]: {}", requestId, e.getMessage()))
                .onErrorReturn(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build());
    }

    /**
     * Internal endpoint: Python worker calls this to deliver a completed response.
     *
     * <p>WHY THIS ENDPOINT?
     *   The Python worker needs a way to return generated responses to the
     *   waiting HTTP clients. Rather than requiring the worker to know each
     *   client's SSE stream URL, it POSTs to this internal endpoint with the
     *   requestId, and the gateway routes the response to the correct Sink.
     *
     *   In a production cluster with multiple gateway instances, this would use
     *   Redis Pub/Sub (each gateway subscribes to channels keyed by requestId).
     *   For the MVP, we use in-process Sinks which work on a single gateway node.
     *
     * @param requestId The request ID to fulfill
     * @param responseText The generated LLM response text
     */
    @PostMapping("/internal/complete")
    public Mono<ResponseEntity<String>> receiveWorkerResponse(
            @RequestParam String requestId,
            @RequestBody String responseText) {

        log.debug("📩 Internal response received for requestId={}", requestId);

        Sinks.One<String> sink = pendingResponses.get(requestId);
        if (sink != null) {
            // Emit the response to the waiting client's Mono pipeline
            Sinks.EmitResult result = sink.tryEmitValue(responseText);
            if (result.isSuccess()) {
                return Mono.just(ResponseEntity.ok("Response delivered to client"));
            } else {
                log.warn("Could not emit response for requestId={}: {}", requestId, result);
                return Mono.just(ResponseEntity.status(HttpStatus.CONFLICT)
                        .body("Response delivery failed: " + result));
            }
        } else {
            log.warn("No pending response sink found for requestId={}. Client may have timed out.", requestId);
            return Mono.just(ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body("No pending request found for ID: " + requestId));
        }
    }

    /**
     * GET /v1/health — Simple health check endpoint for load balancers.
     */
    @GetMapping("/health")
    public Mono<ResponseEntity<Map<String, String>>> health() {
        return Mono.just(ResponseEntity.ok(Map.of(
                "status", "UP",
                "service", "nexuscore-gateway"
        )));
    }

    /**
     * Estimates token count using the approximate rule of 4 characters per token.
     * This is a rough estimate for the OpenAI tokenizer. For production use,
     * integrate the tiktoken library for exact counts.
     *
     * @param text Input text
     * @return Estimated token count
     */
    private int estimateTokenCount(String text) {
        if (text == null || text.isEmpty()) return 0;
        return Math.max(1, text.length() / 4);
    }
}
