package com.example.pulse.ingestion.wikipedia;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.time.Duration;

/**
 * Turns a saved Wikimedia event id into a {@code Last-Event-ID} that is safe to resume from.
 * <p>
 * The ids are not positions in the stream but Kafka timestamps per topic, e.g.
 * {@code [{"topic":"eqiad.mediawiki.recentchange","partition":0,"timestamp":1791110060549}, …]},
 * and the server resumes with the first event <i>after</i> that millisecond. That loses events in
 * two cases, both measured on the live stream (journal 010): several events share a millisecond
 * (about 2% of neighbours), and timestamps sometimes go backwards by a few milliseconds. Resuming a
 * few seconds earlier covers both; the replayed events are duplicates, which the sink ignores.
 */
final class ResumePosition {

	private static final JsonMapper JSON = new JsonMapper();

	private ResumePosition() {
	}

	/**
	 * Moves every timestamp back by {@code margin}. Anything it doesn't understand is returned
	 * unchanged: the server then decides, and rejects it if it is malformed.
	 */
	static String rewind(String lastEventId, Duration margin) {
		if (lastEventId.isEmpty()) {
			return lastEventId;
		}
		JsonNode positions;
		try {
			positions = JSON.readTree(lastEventId);
		}
		catch (JacksonException ex) {
			return lastEventId;
		}
		if (!positions.isArray()) {
			return lastEventId;
		}
		for (JsonNode position : positions) {
			if (position instanceof ObjectNode topic && topic.path("timestamp").isIntegralNumber()) {
				topic.put("timestamp", topic.path("timestamp").longValue() - margin.toMillis());
			}
		}
		return JSON.writeValueAsString(positions);
	}

}
