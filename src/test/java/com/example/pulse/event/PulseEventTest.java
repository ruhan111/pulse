package com.example.pulse.event;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PulseEventTest {

	private static final Instant OCCURRED = Instant.parse("2026-09-30T12:00:00Z");
	private static final Instant INGESTED = Instant.parse("2026-09-30T12:03:00Z");

	@Test
	void sameSourceItemAlwaysHasSameId() {
		PulseEvent first = event(Source.RSS, "item-42", "First title");
		PulseEvent refetched = event(Source.RSS, "item-42", "Edited title");

		assertThat(refetched.id()).isEqualTo(first.id());
	}

	@Test
	void sameExternalIdFromDifferentSourcesIsDifferentEvent() {
		assertThat(event(Source.RSS, "42", "t").id())
			.isNotEqualTo(event(Source.HACKER_NEWS, "42", "t").id());
	}

	@Test
	void stripsTextAndDefaultsOptionalFields() {
		PulseEvent event = new PulseEvent(Source.RSS, " feed ", " 42 ", EventType.PUBLISHED, OCCURRED, INGESTED,
				"  Title  ", URI.create("https://example.com/42"), null, null);

		assertThat(event.channel()).isEqualTo("feed");
		assertThat(event.externalId()).isEqualTo("42");
		assertThat(event.title()).isEqualTo("Title");
		assertThat(event.summary()).isEmpty();
		assertThat(event.attributes()).isEmpty();
	}

	@Test
	void rejectsBlankRequiredText() {
		assertThatThrownBy(() -> event(Source.RSS, "   ", "t"))
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessageContaining("externalId");
		assertThatThrownBy(() -> event(Source.RSS, "42", ""))
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessageContaining("title");
	}

	@Test
	void attributesCannotBeChangedAfterConstruction() {
		Map<String, String> attributes = new HashMap<>(Map.of("score", "10"));
		PulseEvent event = new PulseEvent(Source.HACKER_NEWS, "front", "1", EventType.PUBLISHED, OCCURRED,
				INGESTED, "t", URI.create("https://example.com"), "", attributes);

		attributes.put("score", "999");

		assertThat(event.attributes()).containsEntry("score", "10");
		assertThatThrownBy(() -> event.attributes().put("x", "y"))
			.isInstanceOf(UnsupportedOperationException.class);
	}

	@Test
	void latenessIsTimeBetweenOccurringAndIngesting() {
		assertThat(event(Source.RSS, "42", "t").lateness()).isEqualTo(Duration.ofMinutes(3));
	}

	private static PulseEvent event(Source source, String externalId, String title) {
		return new PulseEvent(source, "https://example.com/feed.xml", externalId, EventType.PUBLISHED, OCCURRED,
				INGESTED, title, URI.create("https://example.com/" + externalId.strip()), "", Map.of());
	}

}
