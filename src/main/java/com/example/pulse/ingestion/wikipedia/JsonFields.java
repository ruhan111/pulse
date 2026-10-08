package com.example.pulse.ingestion.wikipedia;

import tools.jackson.databind.JsonNode;

/**
 * Reads fields of Wikimedia's JSON events by JSON pointer ("/revision/new"), so nested fields read
 * like top-level ones and a missing field is reported by its full path. Strict about types: a
 * {@code "bot": "false"} is missing, not guessed (009).
 */
final class JsonFields {

	private JsonFields() {
	}

	static String requiredText(JsonNode json, String pointer) {
		String value = optionalText(json, pointer);
		if (value.isBlank()) {
			throw new IllegalArgumentException("missing " + pointer);
		}
		return value;
	}

	static String optionalText(JsonNode json, String pointer) {
		JsonNode value = json.at(pointer);
		return value.isString() ? value.stringValue() : "";
	}

	static long requiredLong(JsonNode json, String pointer) {
		JsonNode value = json.at(pointer);
		if (!value.isIntegralNumber()) {
			throw new IllegalArgumentException("missing " + pointer);
		}
		return value.longValue();
	}

	static boolean requiredBoolean(JsonNode json, String pointer) {
		JsonNode value = json.at(pointer);
		if (!value.isBoolean()) {
			throw new IllegalArgumentException("missing " + pointer);
		}
		return value.booleanValue();
	}

}
