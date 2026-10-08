package com.example.pulse.ingestion.wikipedia;

import com.example.pulse.ingestion.wikipedia.Disconnect.Reason;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Metrics of the Wikipedia streams, tagged with the stream. Whether a kept change or an annotation
 * was new is counted where it is stored ({@code pulse.events.accepted}, {@code pulse.annotations.accepted});
 * this covers what only the stream knows: everything received, everything skipped, and how
 * connections end.
 * <p>
 * The counters read the consumer's {@link StreamTally} when metrics are scraped instead of being
 * incremented from the reading loop, so the loop contains no metrics code and nothing is counted
 * twice. Only the end of a connection is recorded explicitly, once per connection, like
 * {@code RssMetrics.polled} at the end of a poll.
 * <p>
 * Every meter exists from startup with 0. Micrometer otherwise creates a meter on its first use, and
 * Actuator answers 404 until then, which looked like a broken endpoint (008).
 */
class WikipediaMetrics {

	/** Window for a timer's max and percentiles; Micrometer's default of 2 minutes misses rare disconnects. */
	private static final Duration STATISTICS_WINDOW = Duration.ofHours(1);

	private final Map<Reason, Timer> connections = new EnumMap<>(Reason.class);

	WikipediaMetrics(MeterRegistry registry, StreamTally tally) {
		requireNonNull(registry, "registry");
		requireNonNull(tally, "tally");
		for (WikipediaStream stream : WikipediaStream.values()) {
			FunctionCounter.builder("pulse.wikipedia.changes.received", tally, counts -> counts.received(stream))
				.description("Events handled from a stream, from every wiki, before filtering")
				.tag("stream", stream.name())
				.register(registry);
			for (SkipReason reason : SkipReason.values()) {
				FunctionCounter
					.builder("pulse.wikipedia.changes.skipped", tally, counts -> counts.skippedCount(stream, reason))
					.description("Handled events that became neither an event nor an annotation")
					.tag("stream", stream.name())
					.tag("reason", reason.name())
					.register(registry);
			}
		}
		for (Reason reason : Reason.values()) {
			connections.put(reason, Timer.builder("pulse.wikipedia.connections")
				.description("Stream connections by why they ended; the timer measures how long each lasted")
				.tag("reason", reason.name())
				.distributionStatisticExpiry(STATISTICS_WINDOW)
				.register(registry));
		}
	}

	void disconnected(Reason reason, Duration connected) {
		connections.get(reason).record(connected);
	}

}
