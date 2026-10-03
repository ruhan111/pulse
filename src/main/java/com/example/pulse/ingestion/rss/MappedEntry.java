package com.example.pulse.ingestion.rss;

import com.example.pulse.event.PulseEvent;

import static java.util.Objects.requireNonNull;

/**
 * The outcome of mapping one feed entry: either an event, or the reason it was skipped.
 * <p>
 * Skipping is an expected result, not an exception. Bad entries are normal in real feeds, and the
 * caller should be able to count them without try/catch.
 */
sealed interface MappedEntry {

	record Mapped(PulseEvent event) implements MappedEntry {

		public Mapped {
			requireNonNull(event, "event");
		}

	}

	record Skipped(SkipReason reason, String detail) implements MappedEntry {

		public Skipped {
			requireNonNull(reason, "reason");
			detail = detail == null ? "" : detail;
		}

	}

}
