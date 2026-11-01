package io.nexuscore.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

/**
 * =============================================================================
 * NexusCoreGatewayApplication — Spring Boot Entry Point
 * =============================================================================
 * WHAT THIS CLASS DOES:
 *   Bootstraps the entire Spring Boot application context. When {@code main()}
 *   is invoked, Spring Boot:
 *     1. Scans all packages under {@code io.nexuscore.gateway} for components
 *     2. Auto-configures WebFlux (Netty), Redis, Kafka, and JPA based on
 *        the dependencies present in pom.xml and application.yml properties
 *     3. Starts an embedded Netty HTTP server on port 8080
 *
 * WHY @SpringBootApplication?
 *   This composite annotation combines:
 *   - @Configuration: marks this as a Spring configuration source
 *   - @EnableAutoConfiguration: triggers Spring Boot's opinionated auto-config
 *   - @ComponentScan: auto-discovers all @Component, @Service, @Repository,
 *     @RestController, and @Configuration beans in sub-packages
 *
 * WHY @EnableAsync?
 *   The KafkaConsumerService uses @Async to process telemetry events without
 *   blocking the Kafka listener thread. @EnableAsync activates Spring's
 *   proxy-based async execution mechanism.
 *
 * STARTUP SEQUENCE:
 *   1. Spring context initializes all beans
 *   2. DataSource (Postgres) pool is established
 *   3. Lettuce Redis client connects to Redis Stack
 *   4. Kafka producer/consumer clients connect to broker
 *   5. Netty HTTP server starts and begins accepting connections on port 8080
 * =============================================================================
 */
@SpringBootApplication
@EnableAsync
public class NexusCoreGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(NexusCoreGatewayApplication.class, args);
    }
}
