package com.example.pulse.ingestion.wikipedia;

import java.util.Optional;

/**
 * Turns the lines of a {@code text/event-stream} into {@link ServerSentEvent}s, following the HTML
 * spec's parsing rules. The JDK has no SSE client, and the format is small enough that a library
 * would cost more than it saves.
 * <p>
 * One parser per connection: it holds the event being assembled and the last event id. Lines must
 * come without their terminator; {@link java.io.BufferedReader#readLine()} already accepts CR, LF
 * and CRLF, as the spec requires.
 */
class SseParser {

	private static final String DEFAULT_TYPE = "message";

	private final StringBuilder data = new StringBuilder();
	private String type = "";
	private String lastEventId = "";

	/** Feeds one line. Returns an event when the line completes one, i.e. on a blank line. */
	Optional<ServerSentEvent> line(String line) {
		if (line.isEmpty()) {
			return dispatch();
		}
		if (line.startsWith(":")) {
			// A comment, e.g. the ":ok" Wikimedia sends first. Servers also use them as keep-alives.
			return Optional.empty();
		}
		int colon = line.indexOf(':');
		String field = colon < 0 ? line : line.substring(0, colon);
		String value = colon < 0 ? "" : line.substring(colon + 1);
		if (value.startsWith(" ")) {
			value = value.substring(1);
		}
		switch (field) {
			case "data" -> data.append(value).append('\n');
			case "event" -> type = value;
			case "id" -> {
				if (value.indexOf('\0') < 0) {
					lastEventId = value;
				}
			}
			// "retry" asks for a reconnect delay. Ignored: the consumer has its own backoff.
			default -> {
			}
		}
		return Optional.empty();
	}

	/**
	 * The id outlives the event: an event without an {@code id:} line resumes from the previous one.
	 * Without data there is no event, and an event cut off by the end of the stream is never
	 * dispatched, so a half-received event is never published.
	 */
	private Optional<ServerSentEvent> dispatch() {
		if (data.isEmpty()) {
			type = "";
			return Optional.empty();
		}
		data.setLength(data.length() - 1);
		ServerSentEvent event = new ServerSentEvent(lastEventId, type.isEmpty() ? DEFAULT_TYPE : type, data.toString());
		data.setLength(0);
		type = "";
		return Optional.of(event);
	}

}
