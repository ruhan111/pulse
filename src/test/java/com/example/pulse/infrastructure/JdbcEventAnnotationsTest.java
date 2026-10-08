package com.example.pulse.infrastructure;

import com.example.pulse.TestDatabase;
import com.example.pulse.event.EventAnnotation;
import com.example.pulse.event.EventAnnotation.Kind;
import com.example.pulse.event.EventId;
import com.example.pulse.event.Source;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Against a real PostgreSQL, like the event sink: the idempotency lives in SQL. */
@SpringBootTest(properties = { "pulse.rss.enabled=false", "pulse.wikipedia.enabled=false" })
class JdbcEventAnnotationsTest {

	private static final Instant REVERTED_AT = Instant.parse("2026-10-08T19:17:41.409Z");

	@DynamicPropertySource
	static void database(DynamicPropertyRegistry registry) {
		TestDatabase.register(registry);
	}

	@Autowired
	private JdbcEventAnnotations annotations;

	@Autowired
	private JdbcClient jdbc;

	@Autowired
	private MeterRegistry registry;

	private final String findAnnotations = SqlFile.load("find_annotations_of_event");

	@Test
	void recordsAFactOnceAndReportsTheRepeat() {
		EventId edit = uniqueEvent();

		assertThat(annotations.annotate(new EventAnnotation(edit, Kind.REVERTED, REVERTED_AT))).isTrue();
		assertThat(annotations.annotate(new EventAnnotation(edit, Kind.REVERTED, REVERTED_AT.plusSeconds(60))))
			.isFalse();

		// The first record wins: annotated_at is when the source first reported it.
		assertThat(stored(edit)).containsExactly(Map.entry("REVERTED", REVERTED_AT));
	}

	@Test
	void anEventCanHaveSeveralKinds() {
		EventId edit = uniqueEvent();

		annotations.annotate(new EventAnnotation(edit, Kind.REVERT, REVERTED_AT));
		annotations.annotate(new EventAnnotation(edit, Kind.REVERTED, REVERTED_AT));

		assertThat(stored(edit)).extracting(Map.Entry::getKey).containsExactly("REVERT", "REVERTED");
	}

	@Test
	void needsNoStoredEvent() {
		// Tags can arrive before their edit; there is no foreign key to events.
		EventId notYetStored = uniqueEvent();

		assertThat(annotations.annotate(new EventAnnotation(notYetStored, Kind.REVERT, REVERTED_AT))).isTrue();
	}

	@Test
	void countsNewAndKnownFactsPerKind() {
		double newBefore = count(Kind.REDIRECT, "NEW");
		double knownBefore = count(Kind.REDIRECT, "DUPLICATE");
		EventId page = uniqueEvent();

		annotations.annotate(new EventAnnotation(page, Kind.REDIRECT, REVERTED_AT));
		annotations.annotate(new EventAnnotation(page, Kind.REDIRECT, REVERTED_AT));

		assertThat(count(Kind.REDIRECT, "NEW") - newBefore).isEqualTo(1);
		assertThat(count(Kind.REDIRECT, "DUPLICATE") - knownBefore).isEqualTo(1);
	}

	private List<Map.Entry<String, Instant>> stored(EventId event) {
		return jdbc.sql(findAnnotations)
			.param(JdbcEventAnnotations.PARAM_EVENT_ID, event.value())
			.query((row, i) -> Map.entry(row.getString("kind"), row.getObject("annotated_at", OffsetDateTime.class)
				.toInstant()))
			.list();
	}

	private double count(Kind kind, String result) {
		return registry.get("pulse.annotations.accepted").tag("kind", kind.name()).tag("result", result).counter()
			.count();
	}

	private static EventId uniqueEvent() {
		return EventId.of(Source.WIKIPEDIA, "enwiki:" + UUID.randomUUID());
	}

}
