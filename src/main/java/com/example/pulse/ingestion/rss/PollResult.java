package com.example.pulse.ingestion.rss;

import java.net.URI;
import java.time.Duration;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * What happened when one feed was polled. Everything a log line or a metric needs.
 *
 * @param published events handed to the sink
 * @param skipped   entries that couldn't become events, counted by reason
 * @param detail    why the poll failed, empty otherwise
 */
record PollResult(URI feed, Outcome outcome, int published, Map<SkipReason, Integer> skipped,
		String detail, Duration duration) {

	enum Outcome {

		/** The feed was fetched, parsed and every event was published. */
		PUBLISHED,

		/** The server answered 304; nothing to do. */
		NOT_MODIFIED,

		FETCH_FAILED,
		PARSE_FAILED,

		/** The sink rejected an event. The feed will be fetched in full again on the next poll. */
		PUBLISH_FAILED

	}

	PollResult {
		requireNonNull(feed, "feed");
		requireNonNull(outcome, "outcome");
		skipped = Map.copyOf(skipped);
		detail = detail == null ? "" : detail;
		requireNonNull(duration, "duration");
	}

	int skippedTotal() {
		return skipped.values().stream().mapToInt(Integer::intValue).sum();
	}

}
