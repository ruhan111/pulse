package com.example.pulse.ingestion.wikipedia;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SseParserTest {

	@Test
	void parsesEventWithIdAndType() {
		assertThat(parse("""
				event: message
				id: 42
				data: {"a": 1}

				""")).containsExactly(new ServerSentEvent("42", "message", "{\"a\": 1}"));
	}

	@Test
	void joinsDataLinesWithNewlines() {
		assertThat(parse("data: one\ndata: two\n\n")).extracting(ServerSentEvent::data).containsExactly("one\ntwo");
	}

	@Test
	void ignoresComments() {
		// Wikimedia's stream starts with ":ok".
		assertThat(parse(":ok\n\ndata: x\n\n")).extracting(ServerSentEvent::data).containsExactly("x");
	}

	@Test
	void defaultsTypeToMessageAndKeepsTheLastIdForEventsWithoutOne() {
		assertThat(parse("id: 1\ndata: a\n\ndata: b\n\n")).containsExactly(
				new ServerSentEvent("1", "message", "a"),
				new ServerSentEvent("1", "message", "b"));
	}

	@Test
	void stripsOnlyOneSpaceAfterTheColon() {
		assertThat(parse("data:no space\n\ndata:  two spaces\n\n")).extracting(ServerSentEvent::data)
			.containsExactly("no space", " two spaces");
	}

	@Test
	void acceptsCrAndCrlfLineEndings() {
		assertThat(parse("id: 1\r\ndata: a\r\n\r\nid: 2\rdata: b\r\r")).extracting(ServerSentEvent::id)
			.containsExactly("1", "2");
	}

	@Test
	void doesNotDispatchAnEventCutOffByTheEndOfTheStream() {
		assertThat(parse("data: complete\n\ndata: cut off")).extracting(ServerSentEvent::data)
			.containsExactly("complete");
	}

	@Test
	void ignoresBlankLinesWithoutDataAndUnknownFields() {
		assertThat(parse("\n\nretry: 1000\nfoo: bar\n\nevent: ping\n\n")).isEmpty();
	}

	@Test
	void parsesTheRealCapture() {
		List<ServerSentEvent> events = parse(RecentChangeMapperTest.resource("recentchange-stream.txt"));

		assertThat(events).hasSize(120);
		assertThat(events).allSatisfy(event -> {
			assertThat(event.type()).isEqualTo("message");
			assertThat(event.id()).startsWith("[{\"topic\":\"eqiad.mediawiki.recentchange\"");
			assertThat(event.data()).startsWith("{\"$schema\":\"/mediawiki/recentchange/1.0.0\"");
		});
	}

	/** Splits lines like the client does, with BufferedReader. */
	private static List<ServerSentEvent> parse(String stream) {
		SseParser parser = new SseParser();
		List<ServerSentEvent> events = new ArrayList<>();
		try (BufferedReader reader = new BufferedReader(new StringReader(stream))) {
			String line;
			while ((line = reader.readLine()) != null) {
				parser.line(line).ifPresent(events::add);
			}
		}
		catch (IOException ex) {
			throw new AssertionError(ex);
		}
		return events;
	}

}
