package com.example.pulse.event;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Something that happened at an external source, normalized into Pulse's common model.
 * <p>
 * Immutable and validated on construction: once a {@code PulseEvent} exists it is known to be
 * well-formed, so no downstream code needs to re-check it.
 *
 * @param source     which kind of system produced the event
 * @param channel    which specific stream within that source, e.g. a feed URL or a subreddit
 * @param externalId the source's own id for the item; together with {@code source} it is the identity
 * @param type       what happened
 * @param occurredAt when the source says it happened. Sources can be wrong, even in the future.
 * @param ingestedAt when Pulse received it, from Pulse's own clock
 * @param title      short human-readable description
 * @param url        where the item can be viewed
 * @param summary    longer text if the source provides one, otherwise empty
 * @param attributes source-specific extras (score, author, stars…) that don't warrant a field of their own
 */
public record PulseEvent(
		Source source,
		String channel,
		String externalId,
		EventType type,
		Instant occurredAt,
		Instant ingestedAt,
		String title,
		URI url,
		String summary,
		Map<String, String> attributes) {

	public PulseEvent {
		requireNonNull(source, "source");
		channel = requireText(channel, "channel");
		externalId = requireText(externalId, "externalId");
		requireNonNull(type, "type");
		requireNonNull(occurredAt, "occurredAt");
		requireNonNull(ingestedAt, "ingestedAt");
		title = requireText(title, "title");
		requireNonNull(url, "url");
		summary = summary == null ? "" : summary.strip();
		attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
	}

	/** Derived rather than stored, so it can never disagree with {@code source} and {@code externalId}. */
	public EventId id() {
		return EventId.of(source, externalId);
	}

	/**
	 * How long after it happened Pulse saw the event. Negative when the source's clock is ahead of
	 * ours. Deciding what to do about that is ingestion policy, not a domain invariant.
	 */
	public Duration lateness() {
		return Duration.between(occurredAt, ingestedAt);
	}

	private static String requireText(String value, String name) {
		requireNonNull(value, name);
		String stripped = value.strip();
		if (stripped.isEmpty()) {
			throw new IllegalArgumentException(name + " must not be blank");
		}
		return stripped;
	}

}
