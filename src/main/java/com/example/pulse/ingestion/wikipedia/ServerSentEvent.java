package com.example.pulse.ingestion.wikipedia;

import static java.util.Objects.requireNonNull;

/**
 * One event from a {@code text/event-stream}, as defined by the HTML spec's server-sent events.
 *
 * @param id   the last event id seen so far, sent back as {@code Last-Event-ID} to resume; empty if none
 * @param type the {@code event:} field, {@code "message"} when the server sends none
 * @param data the {@code data:} lines joined with newlines
 */
record ServerSentEvent(String id, String type, String data) {

	ServerSentEvent {
		requireNonNull(id, "id");
		requireNonNull(type, "type");
		requireNonNull(data, "data");
	}

}
