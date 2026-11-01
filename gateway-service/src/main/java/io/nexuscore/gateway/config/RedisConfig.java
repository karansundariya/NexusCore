package io.nexuscore.gateway.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * =============================================================================
 * RedisConfig — Reactive Redis Client Configuration
 * =============================================================================
 * WHAT THIS DOES:
 *   Configures the Lettuce-based reactive Redis client used throughout the
 *   Gateway. Provides a ReactiveRedisTemplate bean for non-blocking Redis
 *   operations in the WebFlux pipeline.
 *
 * WHY LETTUCE?
 *   Lettuce is the only Redis client that natively supports Project Reactor's
 *   reactive streams API (Mono/Flux). It uses a single shared Netty connection
 *   that multiplexes all commands via pipelining — no connection per thread.
 *   This is critical for a WebFlux gateway that handles thousands of concurrent
 *   requests on a small number of I/O threads.
 *
 * SERIALIZATION STRATEGY:
 *   We use StringRedisSerializer for both keys and values because:
 *   1. Redis HASH fields for VSS are stored as raw bytes (embedded vectors)
 *      and strings (text fields). String serialization preserves byte integrity.
 *   2. Lua scripts that implement rate limiting work with string keys/values.
 *   3. Java objects are serialized to JSON strings before storage (not
 *      Java-serialized binary), making data inspectable via RedisInsight.
 * =============================================================================
 */
@Configuration
public class RedisConfig {

    @Value("${spring.data.redis.host:localhost}")
    private String redisHost;

    @Value("${spring.data.redis.port:6379}")
    private int redisPort;

    /**
     * Creates a ReactiveRedisTemplate configured with String key/value serialization.
     *
     * <p>The ReactiveRedisTemplate is the reactive equivalent of RedisTemplate.
     * It wraps the Lettuce async/reactive commands and returns Mono<T>/Flux<T>.
     * This template is injected into SemanticCacheService and RateLimitFilter.
     *
     * @param connectionFactory Spring auto-configured Lettuce connection factory
     * @return Configured ReactiveRedisTemplate for string operations
     */
    @Bean
    public ReactiveRedisTemplate<String, String> reactiveRedisTemplate(
            ReactiveRedisConnectionFactory connectionFactory) {

        // StringRedisSerializer: encodes keys/values as UTF-8 strings.
        // This is the simplest and most interoperable choice for our use case.
        StringRedisSerializer stringSerializer = new StringRedisSerializer();

        // Build the serialization context: keys=String, values=String, hash-keys=String, hash-values=String
        RedisSerializationContext<String, String> serializationContext =
                RedisSerializationContext.<String, String>newSerializationContext(stringSerializer)
                        .key(stringSerializer)
                        .value(stringSerializer)
                        .hashKey(stringSerializer)
                        .hashValue(stringSerializer)
                        .build();

        return new ReactiveRedisTemplate<>(connectionFactory, serializationContext);
    }
}
