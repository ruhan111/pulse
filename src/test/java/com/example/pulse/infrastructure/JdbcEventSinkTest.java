package com.example.pulse.infrastructure;

import com.example.pulse.TestDatabase;
import com.example.pulse.event.EventSink.Accepted;
import com.example.pulse.event.EventType;
import com.example.pulse.event.PulseEvent;
import com.example.pulse.event.Source;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.net.URI;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/** Against a real PostgreSQL: the idempotency guarantee lives in SQL, so it has to be tested there. */
@SpringBootTest(properties = "pulse.rss.enabled=false")
@ExtendWith(OutputCaptureExtension.class)
class JdbcEventSinkTest {

	private static final Instant OCCURRED = Instant.parse("2026-10-03T12:00:00Z");
	private static final Instant INGESTED = Instant.parse("2026-10-03T12:05:00Z");

	@DynamicPropertySource
	static void database(DynamicPropertyRegistry registry) {
		TestDatabase.register(registry);
	}

	@Autowired
	private JdbcEventSink sink;

	@Autowired
	private JdbcClient jdbc;

	@Test
	void storesANewEventOnceAndReportsTheRepeatAsDuplicate() {
		PulseEvent event = event(uniqueId(), "Title", "", Map.of(), OCCURRED);

		assertThat(sink.accept(event)).isEqualTo(Accepted.NEW);
		assertThat(sink.accept(event)).isEqualTo(Accepted.DUPLICATE);

		assertThat(countWithId(event)).isEqualTo(1);
	}

	@Test
	void keepsTheFirstVersionWhenTheSameIdArrivesAgain() {
		String externalId = uniqueId();
		PulseEvent first = event(externalId, "Original title", "", Map.of(), OCCURRED);
		PulseEvent edited = new PulseEvent(Source.RSS, first.channel(), externalId, EventType.PUBLISHED, OCCURRED,
				INGESTED.plusSeconds(300), "Edited title", first.url(), "", Map.of());

		sink.accept(first);
		assertThat(sink.accept(edited)).isEqualTo(Accepted.DUPLICATE);

		Map<String, Object> row = find(first);
		assertThat(row.get("title")).isEqualTo("Original title");
		// The first sighting is what Phase 5 needs to tell a backfill from fresh activity.
		assertThat(row.get("ingested_at")).isEqualTo(INGESTED);
	}

	@Test
	void storesEveryField() {
		Map<String, String> attributes = Map.of(
				"author", "Zoë \"quoted\" O'Brien",
				"path", "C:\\feeds\\new",
				"emoji", "🚀",
				"empty", "");
		PulseEvent event = event(uniqueId(), "Apple’s Full Disk Access — explained", "A summary.", attributes,
				OCCURRED);

		sink.accept(event);

		Map<String, Object> row = find(event);
		assertThat(row).containsEntry("source", "RSS")
			.containsEntry("channel", "https://example.com/feed.xml")
			.containsEntry("external_id", event.externalId())
			.containsEntry("type", "PUBLISHED")
			.containsEntry("occurred_at", OCCURRED)
			.containsEntry("ingested_at", INGESTED)
			.containsEntry("title", "Apple’s Full Disk Access — explained")
			.containsEntry("url", event.url().toString())
			.containsEntry("summary", "A summary.");
		assertThat(attributes(event)).isEqualTo(attributes);
	}

	@Test
	void storesNoAttributesAsAnEmptyObject() {
		PulseEvent event = event(uniqueId(), "Title", "", Map.of(), OCCURRED);

		sink.accept(event);

		assertThat(attributes(event)).isEmpty();
	}

	@Test
	void roundsTimestampsToMicroseconds() {
		// Java's Instant has nanoseconds, timestamptz only microseconds. Postgres rounds rather than truncates.
		Instant nanos = Instant.parse("2026-10-03T12:00:00.123456789Z");
		PulseEvent event = event(uniqueId(), "Title", "", Map.of(), nanos);

		sink.accept(event);

		assertThat(find(event).get("occurred_at")).isEqualTo(Instant.parse("2026-10-03T12:00:00.123457Z"));
	}

	@Test
	void concurrentWritersOfTheSameEventStoreItExactlyOnce() throws Exception {
		PulseEvent event = event(uniqueId(), "Title", "", Map.of(), OCCURRED);
		List<Callable<Accepted>> writers = new ArrayList<>();
		for (int i = 0; i < 8; i++) {
			writers.add(() -> sink.accept(event));
		}

		List<Accepted> results = new ArrayList<>();
		try (ExecutorService pool = Executors.newFixedThreadPool(writers.size())) {
			for (Future<Accepted> result : pool.invokeAll(writers)) {
				results.add(result.get());
			}
		}

		assertThat(results).containsOnlyOnce(Accepted.NEW);
		assertThat(countWithId(event)).isEqualTo(1);
	}

	@Test
	void logsOnlyNewEvents(CapturedOutput output) {
		String title = "Logged once " + uniqueId();
		PulseEvent event = event(uniqueId(), title, "", Map.of(), OCCURRED);

		sink.accept(event);
		sink.accept(event);

		assertThat(output.getOut()).containsOnlyOnce(title);
	}

	private int countWithId(PulseEvent event) {
		return jdbc.sql(SqlFile.load("count_events_with_id"))
			.param("id", event.id().value())
			.query(Integer.class)
			.single();
	}

	private Map<String, Object> find(PulseEvent event) {
		return jdbc.sql(SqlFile.load("find_event_by_id"))
			.param("id", event.id().value())
			.query((rs, rowNum) -> {
				Map<String, Object> row = new HashMap<>();
				for (String column : List.of("source", "channel", "external_id", "type", "title", "url", "summary")) {
					row.put(column, rs.getString(column));
				}
				row.put("occurred_at", rs.getObject("occurred_at", OffsetDateTime.class).toInstant());
				row.put("ingested_at", rs.getObject("ingested_at", OffsetDateTime.class).toInstant());
				return row;
			})
			.single();
	}

	private Map<String, String> attributes(PulseEvent event) {
		Map<String, String> attributes = new HashMap<>();
		jdbc.sql(SqlFile.load("find_event_attributes_by_id"))
			.param("id", event.id().value())
			.query(rs -> {
				attributes.put(rs.getString("key"), rs.getString("value"));
			});
		return attributes;
	}

	private static PulseEvent event(String externalId, String title, String summary, Map<String, String> attributes,
			Instant occurredAt) {
		return new PulseEvent(Source.RSS, "https://example.com/feed.xml", externalId, EventType.PUBLISHED, occurredAt,
				INGESTED, title, URI.create("https://example.com/" + externalId), summary, attributes);
	}

	/** Every test uses its own ids, so tests sharing the database never see each other's rows. */
	private static String uniqueId() {
		return UUID.randomUUID().toString();
	}

}
