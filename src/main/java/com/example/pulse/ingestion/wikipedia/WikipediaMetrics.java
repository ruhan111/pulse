package com.example.pulse.ingestion.wikipedia;

import com.example.pulse.ingestion.wikipedia.Disconnect.Reason;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Metrics of the Wikipedia stream. Whether a kept change was new or a duplicate is counted by the
 * sink ({@code pulse.events.accepted}, {@code source=WIKIPEDIA}); this covers what only the stream
 * knows: everything received, everything skipped, and how connections end.
 * <p>
 * Every counter is registered at 0 on startup. Micrometer otherwise creates a meter on its first
 * increment, and Actuator answers 404 until then, which looked like a broken endpoint (008).
 */
class WikipediaMetrics {

	/** Window for a timer's max and percentiles; Micrometer's default of 2 minutes misses rare disconnects. */
	private static final Duration STATISTICS_WINDOW = Duration.ofHours(1);

	private final Counter received;
	private final Map<SkipReason, Counter> skipped = new EnumMap<>(SkipReason.class);
	private final Map<Reason, Timer> connections = new EnumMap<>(Reason.class);

	WikipediaMetrics(MeterRegistry registry) {
		requireNonNull(registry, "registry");
		received = Counter.builder("pulse.wikipedia.changes.received")
			.description("Changes received from the stream, from every wiki, before filtering")
			.register(registry);
		for (SkipReason reason : SkipReason.values()) {
			skipped.put(reason, Counter.builder("pulse.wikipedia.changes.skipped")
				.description("Received changes that did not become events")
				.tag("reason", reason.name())
				.register(registry));
		}
		for (Reason reason : Reason.values()) {
			connections.put(reason, Timer.builder("pulse.wikipedia.connections")
				.description("Stream connections by why they ended; the timer measures how long each lasted")
				.tag("reason", reason.name())
				.distributionStatisticExpiry(STATISTICS_WINDOW)
				.register(registry));
		}
	}

	void received() {
		received.increment();
	}

	void skipped(SkipReason reason) {
		skipped.get(reason).increment();
	}

	void disconnected(Reason reason, Duration connected) {
		connections.get(reason).record(connected);
	}

}
