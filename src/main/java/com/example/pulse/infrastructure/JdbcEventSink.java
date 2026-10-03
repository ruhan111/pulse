package com.example.pulse.infrastructure;

import com.example.pulse.event.EventSink;
import com.example.pulse.event.PulseEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static java.util.Objects.requireNonNull;

/**
 * Stores events in PostgreSQL. Idempotency comes from the database: the primary key is the
 * {@code EventId}, and a single {@code insert ... on conflict do nothing} both stores a new event
 * and detects a duplicate, so there is no check-then-insert race between concurrent writers.
 * <p>
 * Failures (database down, timeout) are thrown, not swallowed: the poller then keeps the feed's old
 * cache validators and re-sends everything next round, which is safe because duplicates are ignored.
 */
@Component
class JdbcEventSink implements EventSink {

	private static final Logger log = LoggerFactory.getLogger(JdbcEventSink.class);

	private final JdbcClient jdbc;
	private final String insertEventIfAbsent = SqlFile.load("insert_event_if_absent");

	JdbcEventSink(JdbcClient jdbc) {
		this.jdbc = requireNonNull(jdbc, "jdbc");
	}

	@Override
	public Accepted accept(PulseEvent event) {
		// Keys and values as two parallel arrays, turned into jsonb by Postgres itself: no JSON
		// library, and no hand-written escaping to get wrong.
		String[] keys = event.attributes().keySet().toArray(String[]::new);
		String[] values = new String[keys.length];
		for (int i = 0; i < keys.length; i++) {
			values[i] = event.attributes().get(keys[i]);
		}

		int inserted = jdbc.sql(insertEventIfAbsent)
			.param("id", event.id().value())
			.param("source", event.source().name())
			.param("channel", event.channel())
			.param("externalId", event.externalId())
			.param("type", event.type().name())
			.param("occurredAt", utc(event.occurredAt()))
			.param("ingestedAt", utc(event.ingestedAt()))
			.param("title", event.title())
			.param("url", event.url().toString())
			.param("summary", event.summary())
			.param("attributeKeys", keys)
			.param("attributeValues", values)
			.update();
		if (inserted == 0) {
			return Accepted.DUPLICATE;
		}
		log.info("new event {} {} | {} | {}", event.source(), event.occurredAt(), event.title(), event.url());
		return Accepted.NEW;
	}

	/** The JDBC driver maps {@code OffsetDateTime} to {@code timestamptz}; it has no mapping for {@code Instant}. */
	private static OffsetDateTime utc(Instant instant) {
		return instant.atOffset(ZoneOffset.UTC);
	}

}
