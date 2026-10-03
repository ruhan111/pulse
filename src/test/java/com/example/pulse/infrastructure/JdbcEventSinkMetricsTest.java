package com.example.pulse.infrastructure;

import com.example.pulse.TestDatabase;
import com.example.pulse.event.EventType;
import com.example.pulse.event.PulseEvent;
import com.example.pulse.event.Source;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * The sink's metrics. Each test builds its own sink with a fresh registry, so counts start at zero
 * no matter what else ran against the shared database.
 */
@SpringBootTest(properties = "pulse.rss.enabled=false")
class JdbcEventSinkMetricsTest {

	private static final Instant OCCURRED = Instant.parse("2026-10-03T12:00:00Z");
	private static final Instant INGESTED = Instant.parse("2026-10-03T12:05:00Z");

	@DynamicPropertySource
	static void database(DynamicPropertyRegistry registry) {
		TestDatabase.register(registry);
	}

	@Autowired
	private JdbcClient jdbc;

	private final MeterRegistry registry = new SimpleMeterRegistry();

	@Test
	void countsNewAndDuplicateEventsPerSource() {
		JdbcEventSink sink = new JdbcEventSink(jdbc, registry);
		PulseEvent event = event(OCCURRED);

		sink.accept(event);
		sink.accept(event);
		sink.accept(event);

		assertThat(accepted("NEW")).isEqualTo(1);
		assertThat(accepted("DUPLICATE")).isEqualTo(2);
		assertThat(registry.get("pulse.sink.insert").tag("result", "NEW").timer().count()).isEqualTo(1);
		assertThat(registry.get("pulse.sink.insert").tag("result", "DUPLICATE").timer().count()).isEqualTo(2);
	}

	@Test
	void recordsHowLateNewEventsArriveButIgnoresDuplicates() {
		JdbcEventSink sink = new JdbcEventSink(jdbc, registry);
		PulseEvent event = event(OCCURRED);

		sink.accept(event);
		sink.accept(event);

		var lateness = registry.get("pulse.events.lateness").tag("source", "RSS").timer();
		assertThat(lateness.count()).isEqualTo(1);
		assertThat(lateness.totalTime(TimeUnit.SECONDS)).isEqualTo(Duration.ofMinutes(5).toSeconds());
	}

	@Test
	void countsEventsDatedInTheFutureInsteadOfLosingThem() {
		JdbcEventSink sink = new JdbcEventSink(jdbc, registry);

		sink.accept(event(INGESTED.plusSeconds(3600)));

		assertThat(registry.get("pulse.events.future").tag("source", "RSS").counter().count()).isEqualTo(1);
		assertThat(registry.find("pulse.events.lateness").timer()).isNull();
	}

	@Test
	void recordsFailedInsertsAndStillThrows() {
		// Nothing listens on port 9, so every insert fails, like a database that is down.
		JdbcClient unreachable = JdbcClient.create(new DriverManagerDataSource("jdbc:postgresql://127.0.0.1:9/pulse"));
		JdbcEventSink sink = new JdbcEventSink(unreachable, registry);

		assertThatExceptionOfType(DataAccessException.class).isThrownBy(() -> sink.accept(event(OCCURRED)));

		assertThat(registry.get("pulse.sink.insert").tag("result", "FAILED").timer().count()).isEqualTo(1);
		assertThat(registry.find("pulse.events.accepted").counter()).isNull();
	}

	private double accepted(String result) {
		return registry.get("pulse.events.accepted").tag("source", "RSS").tag("result", result).counter().count();
	}

	private static PulseEvent event(Instant occurredAt) {
		String externalId = UUID.randomUUID().toString();
		return new PulseEvent(Source.RSS, "https://example.com/feed.xml", externalId, EventType.PUBLISHED, occurredAt,
				INGESTED, "Title", URI.create("https://example.com/" + externalId), "", Map.of());
	}

}
