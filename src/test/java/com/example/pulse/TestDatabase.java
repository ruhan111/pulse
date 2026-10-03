package com.example.pulse;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.util.Map;

/**
 * One PostgreSQL container for the whole test run, same image as compose.yaml. Started on first use
 * and removed by Testcontainers when the JVM exits. Shared rather than per test class, so several
 * Spring contexts (e.g. a restart) can see the same data, and the container starts only once.
 */
public final class TestDatabase {

	private static final PostgreSQLContainer POSTGRES = start();

	private TestDatabase() {
	}

	/** Points a Spring test at the container; call from a {@code @DynamicPropertySource} method. */
	public static void register(DynamicPropertyRegistry registry) {
		registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
		registry.add("spring.datasource.username", POSTGRES::getUsername);
		registry.add("spring.datasource.password", POSTGRES::getPassword);
	}

	/** The same settings as properties, for tests that start the application themselves. */
	public static Map<String, Object> properties() {
		return Map.of(
				"spring.datasource.url", POSTGRES.getJdbcUrl(),
				"spring.datasource.username", POSTGRES.getUsername(),
				"spring.datasource.password", POSTGRES.getPassword());
	}

	private static PostgreSQLContainer start() {
		PostgreSQLContainer container = new PostgreSQLContainer("postgres:17-alpine");
		container.start();
		return container;
	}

}
