package com.learnkafka;

import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Shared Testcontainers configuration — holds a single PostgreSQL container
 * that is started once per JVM and reused across all integration test classes
 * via @ImportTestcontainers(TestcontainersConfiguration.class).
 */
public class TestcontainersConfiguration {

    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:latest");
}
