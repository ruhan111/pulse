package com.example.pulse.infrastructure;

import com.example.pulse.event.EventSink.Accepted;
import com.example.pulse.event.EventType;
import com.example.pulse.event.PulseEvent;
import com.example.pulse.event.Source;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import java.net.URI;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(OutputCaptureExtension.class)
class LoggingEventSinkTest {

	private final LoggingEventSink sink = new LoggingEventSink();

	@Test
	void reportsAndLogsEachEventOnlyOnce(CapturedOutput output) {
		assertThat(sink.accept(event("1", "First"))).isEqualTo(Accepted.NEW);
		assertThat(sink.accept(event("1", "First"))).isEqualTo(Accepted.DUPLICATE);
		assertThat(sink.accept(event("2", "Second"))).isEqualTo(Accepted.NEW);

		assertThat(output.getOut()).containsOnlyOnce("First").containsOnlyOnce("Second");
	}

	private static PulseEvent event(String externalId, String title) {
		Instant now = Instant.parse("2026-10-03T12:00:00Z");
		return new PulseEvent(Source.RSS, "https://example.com/feed.xml", externalId, EventType.PUBLISHED, now, now,
				title, URI.create("https://example.com/" + externalId), "", Map.of());
	}

}
