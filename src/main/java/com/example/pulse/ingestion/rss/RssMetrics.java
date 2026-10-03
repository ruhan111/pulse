package com.example.pulse.ingestion.rss;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.net.URI;
import java.time.Duration;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Turns poll results into metrics. The result types were designed for this: {@link PollResult.Outcome},
 * {@link SkipReason} and {@link FetchFailure} are enums, so they become tag values with a fixed set
 * of possibilities.
 * <p>
 * The feed URL is a tag too. That is safe because feeds come from configuration, so the number of
 * time series stays bounded; event ids or titles must never become tags.
 */
class RssMetrics {

	/** Window for a timer's max. Micrometer's default of 2 minutes is shorter than the poll interval. */
	private static final Duration STATISTICS_WINDOW = Duration.ofHours(1);

	private final MeterRegistry registry;

	RssMetrics(MeterRegistry registry) {
		this.registry = requireNonNull(registry, "registry");
	}

	void polled(PollResult result) {
		Timer.builder("pulse.rss.polls")
			.description("Polls of one feed, by outcome; the timer measures the whole poll")
			.tag("feed", result.feed().toString())
			.tag("outcome", result.outcome().name())
			.distributionStatisticExpiry(STATISTICS_WINDOW)
			.register(registry)
			.record(result.duration());
		for (Map.Entry<SkipReason, Integer> skipped : result.skipped().entrySet()) {
			Counter.builder("pulse.rss.entries.skipped")
				.description("Feed entries that could not become events")
				.tag("feed", result.feed().toString())
				.tag("reason", skipped.getKey().name())
				.register(registry)
				.increment(skipped.getValue());
		}
	}

	void fetchFailed(URI feed, FetchFailure reason) {
		Counter.builder("pulse.rss.fetch.failures")
			.description("Failed feed fetches, by reason")
			.tag("feed", feed.toString())
			.tag("reason", reason.name())
			.register(registry)
			.increment();
	}

}
