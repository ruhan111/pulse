package com.example.pulse.event;

import java.time.Instant;

import static java.util.Objects.requireNonNull;

/**
 * A fact learned about an event after it happened, e.g. that a Wikipedia edit was later reverted.
 * <p>
 * Kept apart from {@link PulseEvent} because events are stored once and never rewritten (the first
 * version wins), while such facts can arrive seconds or minutes later, and even before the event
 * itself.
 *
 * @param eventId     the event the fact is about; it may not have been stored yet
 * @param kind        what was learned
 * @param annotatedAt when the source recorded the fact
 */
public record EventAnnotation(EventId eventId, Kind kind, Instant annotatedAt) {

	public EventAnnotation {
		requireNonNull(eventId, "eventId");
		requireNonNull(kind, "kind");
		requireNonNull(annotatedAt, "annotatedAt");
	}

	/**
	 * Deliberately small, like {@link EventType}: a kind earns its place when something treats it
	 * differently. Trend detection ignores reverts, reverted edits and redirect creations.
	 */
	public enum Kind {

		/** The event undid earlier ones, e.g. a rollback or an undo. Clean-up, not interest. */
		REVERT,

		/** The event was undone later, typically vandalism. */
		REVERTED,

		/** The event created a page that only redirects to another one: a new title, not new content. */
		REDIRECT

	}

}
