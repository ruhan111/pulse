package com.example.pulse.infrastructure;

import com.example.pulse.event.EventSink;
import com.example.pulse.event.PulseEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import java.time.Duration;
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
 * <p>
 * Records what happened to each event as metrics: new or duplicate per source, insert latency, and
 * how late new events arrive. The sink is the only place that knows whether an event is new.
 */
@Component
class JdbcEventSink implements EventSink {

	/**
	 * Named parameters of the event SQL files ({@code :id}, {@code :externalId}, …). One spelling,
	 * so a query file and the Java that binds it can't drift apart.
	 */
	static final String PARAM_ID = "id";
	static final String PARAM_SOURCE = "source";
	static final String PARAM_CHANNEL = "channel";
	static final String PARAM_EXTERNAL_ID = "externalId";
	static final String PARAM_TYPE = "type";
	static final String PARAM_OCCURRED_AT = "occurredAt";
	static final String PARAM_INGESTED_AT = "ingestedAt";
	static final String PARAM_TITLE = "title";
	static final String PARAM_URL = "url";
	static final String PARAM_SUMMARY = "summary";
	static final String PARAM_ATTRIBUTE_KEYS = "attributeKeys";
	static final String PARAM_ATTRIBUTE_VALUES = "attributeValues";

	private static final Logger log = LoggerFactory.getLogger(JdbcEventSink.class);

	/**
	 * Percentiles and max cover a sliding window. Micrometer's default is 2 minutes, which would be
	 * empty most of the time with polls every 5 minutes.
	 */
	private static final Duration STATISTICS_WINDOW = Duration.ofHours(1);

	private final JdbcClient jdbc;
	private final MeterRegistry registry;
	private final String insertEventIfAbsent = SqlFile.load("insert_event_if_absent");

	JdbcEventSink(JdbcClient jdbc, MeterRegistry registry) {
		this.jdbc = requireNonNull(jdbc, "jdbc");
		this.registry = requireNonNull(registry, "registry");
	}

	@Override
	public Accepted accept(PulseEvent event) {
		Timer.Sample sample = Timer.start(registry);
		Accepted accepted;
		try {
			accepted = insert(event) == 0 ? Accepted.DUPLICATE : Accepted.NEW;
		}
		catch (RuntimeException ex) {
			sample.stop(insertTimer("FAILED"));
			throw ex;
		}
		sample.stop(insertTimer(accepted.name()));

		Counter.builder("pulse.events.accepted")
			.description("Events handed to the sink, by whether they were new")
			.tag("source", event.source().name())
			.tag("result", accepted.name())
			.register(registry)
			.increment();
		if (accepted == Accepted.NEW) {
			recordLateness(event);
			log.info("new event {} {} | {} | {}", event.source(), event.occurredAt(), event.title(), event.url());
		}
		return accepted;
	}

	/** Returns the number of rows inserted: 1 for a new event, 0 for a duplicate. */
	private int insert(PulseEvent event) {
		// Keys and values as two parallel arrays, turned into jsonb by Postgres itself: no JSON
		// library, and no hand-written escaping to get wrong.
		String[] keys = event.attributes().keySet().toArray(String[]::new);
		String[] values = new String[keys.length];
		for (int i = 0; i < keys.length; i++) {
			values[i] = event.attributes().get(keys[i]);
		}

		return jdbc.sql(insertEventIfAbsent)
			.param(PARAM_ID, event.id().value())
			.param(PARAM_SOURCE, event.source().name())
			.param(PARAM_CHANNEL, event.channel())
			.param(PARAM_EXTERNAL_ID, event.externalId())
			.param(PARAM_TYPE, event.type().name())
			.param(PARAM_OCCURRED_AT, utc(event.occurredAt()))
			.param(PARAM_INGESTED_AT, utc(event.ingestedAt()))
			.param(PARAM_TITLE, event.title())
			.param(PARAM_URL, event.url().toString())
			.param(PARAM_SUMMARY, event.summary())
			.param(PARAM_ATTRIBUTE_KEYS, keys)
			.param(PARAM_ATTRIBUTE_VALUES, values)
			.update();
	}

	private Timer insertTimer(String result) {
		return Timer.builder("pulse.sink.insert")
			.description("Time to store one event, by result (NEW, DUPLICATE, FAILED)")
			.tag("result", result)
			.publishPercentiles(0.5, 0.99)
			.distributionStatisticExpiry(STATISTICS_WINDOW)
			.register(registry);
	}

	/**
	 * How old a new event was when Pulse first stored it: the input for the freshness SLO, and the
	 * way to tell a backfill (days old) from live activity (minutes old). Only new events count;
	 * a duplicate's age says nothing about freshness.
	 * <p>
	 * Timers ignore negative durations, so events dated in the future (a source's clock is ahead)
	 * are counted separately instead of disappearing.
	 */
	private void recordLateness(PulseEvent event) {
		Duration lateness = event.lateness();
		if (lateness.isNegative()) {
			Counter.builder("pulse.events.future")
				.description("New events whose source dated them in the future")
				.tag("source", event.source().name())
				.register(registry)
				.increment();
			return;
		}
		Timer.builder("pulse.events.lateness")
			.description("Age of a new event when first stored (ingestedAt - occurredAt)")
			.tag("source", event.source().name())
			.publishPercentiles(0.5, 0.99)
			.distributionStatisticExpiry(STATISTICS_WINDOW)
			.register(registry)
			.record(lateness);
	}

	/** The JDBC driver maps {@code OffsetDateTime} to {@code timestamptz}; it has no mapping for {@code Instant}. */
	private static OffsetDateTime utc(Instant instant) {
		return instant.atOffset(ZoneOffset.UTC);
	}

}
