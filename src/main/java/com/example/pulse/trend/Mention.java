package com.example.pulse.trend;

import com.example.pulse.event.EventId;
import com.example.pulse.event.PulseEvent;
import com.example.pulse.event.Source;

import java.time.Instant;

import static java.util.Objects.requireNonNull;

/**
 * One occurrence of a {@link Topic} in one event: the unit trend detection counts.
 * <p>
 * Keeps a reference back to its event so a trend can explain itself ("rust is up 4×, and these are
 * the stories behind it"), and its source so agreement between independent sources can be scored.
 */
public record Mention(Topic topic, EventId eventId, Source source, Instant occurredAt) {

	public Mention {
		requireNonNull(topic, "topic");
		requireNonNull(eventId, "eventId");
		requireNonNull(source, "source");
		requireNonNull(occurredAt, "occurredAt");
	}

	public static Mention of(Topic topic, PulseEvent event) {
		return new Mention(topic, event.id(), event.source(), event.occurredAt());
	}

}
