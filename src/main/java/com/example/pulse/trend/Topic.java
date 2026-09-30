package com.example.pulse.trend;

import java.util.Locale;

import static java.util.Objects.requireNonNull;

/**
 * The thing that trends: the key Pulse counts activity for.
 * <p>
 * Normalized on construction ("  Rust ", "rust" and "RUST" are the same topic), so equality is
 * exactly the counting key. Getting this key right matters more than any statistics built on it.
 */
public record Topic(TopicKind kind, String value) {

	public Topic {
		requireNonNull(kind, "kind");
		requireNonNull(value, "value");
		value = value.strip().replaceAll("\\s+", " ").toLowerCase(Locale.ROOT);
		if (value.isEmpty()) {
			throw new IllegalArgumentException("value must not be blank");
		}
	}

	public static Topic term(String value) {
		return new Topic(TopicKind.TERM, value);
	}

	public static Topic domain(String value) {
		return new Topic(TopicKind.DOMAIN, value);
	}

}
