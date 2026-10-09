package com.example.pulse.ingestion.wikipedia;

import com.example.pulse.event.EventAnnotation;
import com.example.pulse.event.PulseEvent;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * The outcome of mapping one event from Wikimedia's streams: an event (a recent change), annotations
 * about events (a tag change), or the reason it was skipped.
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

	record Annotated(List<EventAnnotation> annotations) implements MappedChange {

		public Annotated {
			annotations = List.copyOf(annotations);
			if (annotations.isEmpty()) {
				throw new IllegalArgumentException("annotations must not be empty; skip instead");
			}
		}

	}

	record Skipped(SkipReason reason, String detail) implements MappedChange {

		public Skipped {
			requireNonNull(reason, "reason");
			detail = detail == null ? "" : detail;
		}

	}

}
