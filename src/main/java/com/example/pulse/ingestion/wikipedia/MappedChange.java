package com.example.pulse.ingestion.wikipedia;

import com.example.pulse.event.PulseEvent;

import static java.util.Objects.requireNonNull;

/**
 * The outcome of mapping one recent change: either an event, or the reason it was skipped.
 * <p>
 * Most of the stream is skipped on purpose (other wikis, bots, talk pages), so skipping is the
 * normal case, not an error.
 */
sealed interface MappedChange {

	record Mapped(PulseEvent event) implements MappedChange {

		public Mapped {
			requireNonNull(event, "event");
		}

	}

	record Skipped(SkipReason reason, String detail) implements MappedChange {

		public Skipped {
			requireNonNull(reason, "reason");
			detail = detail == null ? "" : detail;
		}

	}

}
