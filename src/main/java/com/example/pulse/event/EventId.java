package com.example.pulse.event;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

/**
 * Identity of a {@link PulseEvent}, derived deterministically from {@code (source, externalId)}.
 * <p>
 * Seeing the same item twice (re-polling a feed, a retried request, a replayed stream) always
 * yields the same id. Deduplication therefore starts in the domain; storage only has to enforce
 * uniqueness on the id.
 */
public record EventId(UUID value) {

	public EventId {
		requireNonNull(value, "value");
	}

	public static EventId of(Source source, String externalId) {
		requireNonNull(source, "source");
		requireNonNull(externalId, "externalId");
		// Source names never contain ':', so the separator cannot make two keys collide.
		byte[] key = (source.name() + ":" + externalId).getBytes(StandardCharsets.UTF_8);
		return new EventId(UUID.nameUUIDFromBytes(key));
	}

	@Override
	public String toString() {
		return value.toString();
	}

}
