package io.nexuscore.gateway;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * =============================================================================
 * AbstractIntegrationTest — Base Class for All Integration Tests
 * =============================================================================
 * WHAT THIS PROVIDES:
 *   A common base class that all integration test classes extend. It:
 *   1. Activates the "test" Spring profile (loads application-test.yml)
 *   2. Configures test infrastructure (embedded Redis, embedded Kafka, H2 DB)
 *   3. Provides shared @DynamicPropertySource to inject test config into Spring
 *
 * WHY A BASE CLASS?
 *   Without a base class, every test would repeat the same infrastructure
 *   setup annotations. Changes to test infrastructure (e.g., switching from
 *   embedded Redis to Testcontainers) only need to happen here.
 *
 * TEST PROFILE CONFIGURATION (application-test.yml):
 *   - nexuscore.rate-limit.default-capacity: 60 (matches rate limit tests)
 *   - nexuscore.embedding.use-mock: true (no OpenAI API calls in tests)
 *   - spring.datasource: H2 in-memory DB (no Postgres needed in unit tests)
 *   - spring.kafka: embedded Kafka broker
 *   - spring.data.redis: embedded Redis on a random port
 * =============================================================================
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
public abstract class AbstractIntegrationTest {

    /**
     * Injects dynamic properties from the test infrastructure into Spring context.
     *
     * <p>For MVP testing, we use Spring's embedded Kafka and a mock Redis configuration.
     * These properties override what's in application-test.yml at runtime.
     *
     * @param registry The Spring dynamic property registry
     */
    @DynamicPropertySource
    static void configureTestProperties(DynamicPropertyRegistry registry) {
        // Use mock embeddings — no OpenAI API key required in tests
        registry.add("nexuscore.embedding.use-mock", () -> "true");

        // Rate limit for tests: 60 requests per 60-second window
        registry.add("nexuscore.rate-limit.default-capacity", () -> "60");
        registry.add("nexuscore.rate-limit.window-seconds", () -> "60");

        // Use an in-memory H2 database instead of PostgreSQL for unit tests
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:mem:nexuscore_test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=false");
        registry.add("spring.datasource.driver-class-name", () -> "org.h2.Driver");
        registry.add("spring.datasource.username", () -> "sa");
        registry.add("spring.datasource.password", () -> "");
        registry.add("spring.jpa.database-platform", () -> "org.hibernate.dialect.H2Dialect");
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");

        // Disable Kafka auto-configuration in tests (use embedded broker from @EmbeddedKafka)
        // The embedded Kafka broker URL is injected by @EmbeddedKafka in specific test classes
    }
}
