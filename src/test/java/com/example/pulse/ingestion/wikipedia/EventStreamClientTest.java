package com.example.pulse.ingestion.wikipedia;

import com.example.pulse.ingestion.wikipedia.Disconnect.Reason;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.example.pulse.ingestion.wikipedia.StreamServer.events;
import static com.example.pulse.ingestion.wikipedia.StreamServer.eventsThenSilence;
import static com.example.pulse.ingestion.wikipedia.StreamServer.status;
import static org.assertj.core.api.Assertions.assertThat;

class EventStreamClientTest {

	private static final Duration IDLE_TIMEOUT = Duration.ofMillis(500);

	private final StreamServer server = new StreamServer();
	private final EventStreamClient client = new EventStreamClient(server.url(), Duration.ofSeconds(5), IDLE_TIMEOUT);
	private final List<ServerSentEvent> received = new ArrayList<>();

	EventStreamClientTest() throws IOException {
	}

	@AfterEach
	void stopServer() {
		server.close();
	}

	@Test
	void readsTheWholeStreamUntilTheServerEndsIt() {
		server.then(events(RecentChangeMapperTest.resource("recentchange-stream.txt")));

		Disconnect disconnect = client.read("", received::add);

		assertThat(disconnect.reason()).isEqualTo(Reason.ENDED);
		assertThat(received).hasSize(120);
	}

	@Test
	void sendsUserAgentAndAcceptButNoLastEventIdWhenStartingFromNow() {
		server.then(events(""));

		client.read("", received::add);

		assertThat(server.requests()).singleElement().satisfies(headers -> {
			assertThat(headers.getFirst("User-Agent")).isEqualTo(EventStreamClient.USER_AGENT);
			assertThat(headers.getFirst("Accept")).isEqualTo("text/event-stream");
			assertThat(headers.containsKey("Last-Event-ID")).isFalse();
		});
	}

	@Test
	void sendsLastEventIdToResume() {
		server.then(events(""));

		client.read("[{\"topic\":\"t\",\"partition\":0,\"timestamp\":1}]", received::add);

		assertThat(server.requests().getFirst().getFirst("Last-Event-ID"))
			.isEqualTo("[{\"topic\":\"t\",\"partition\":0,\"timestamp\":1}]");
	}

	@Test
	void reportsRejectedPositionOnlyFor400ToAResume() {
		server.then(status(400)).then(status(400));

		assertThat(client.read("garbage", received::add).reason()).isEqualTo(Reason.REJECTED_POSITION);
		assertThat(client.read("", received::add).reason()).isEqualTo(Reason.HTTP_ERROR);
	}

	@Test
	void reportsOtherStatusesAsHttpError() {
		server.then(status(503));

		Disconnect disconnect = client.read("", received::add);

		assertThat(disconnect.reason()).isEqualTo(Reason.HTTP_ERROR);
		assertThat(disconnect.detail()).isEqualTo("status 503");
	}

	@Test
	void reportsNetworkErrorWhenNothingListens() {
		URI closed = server.url();
		server.close();

		Disconnect disconnect = new EventStreamClient(closed, Duration.ofSeconds(5), IDLE_TIMEOUT).read("", received::add);

		assertThat(disconnect.reason()).isEqualTo(Reason.NETWORK_ERROR);
	}

	@Test
	void closesASilentConnectionAfterTheIdleTimeout() {
		server.then(eventsThenSilence("id: 1\ndata: a\n\n"));

		long start = System.nanoTime();
		Disconnect disconnect = client.read("", received::add);
		Duration took = Duration.ofNanos(System.nanoTime() - start);

		assertThat(disconnect.reason()).isEqualTo(Reason.IDLE_TIMEOUT);
		assertThat(received).hasSize(1);
		// Detected between one and about 1.25 idle timeouts; the server would stay silent for 60 s.
		assertThat(took).isBetween(IDLE_TIMEOUT, IDLE_TIMEOUT.multipliedBy(4));
	}

	@Test
	void aSlowHandlerIsNotADeadConnection() {
		server.then(events("data: a\n\ndata: b\n\n"));

		Disconnect disconnect = client.read("", event -> {
			sleep(IDLE_TIMEOUT.multipliedBy(3));
			received.add(event);
		});

		assertThat(disconnect.reason()).isEqualTo(Reason.ENDED);
		assertThat(received).hasSize(2);
	}

	@Test
	void stopsReadingWhenTheHandlerFails() {
		server.then(events("data: a\n\ndata: b\n\ndata: c\n\n"));

		Disconnect disconnect = client.read("", event -> {
			if (event.data().equals("b")) {
				throw new IllegalStateException("database down");
			}
			received.add(event);
		});

		assertThat(disconnect.reason()).isEqualTo(Reason.HANDLER_FAILED);
		assertThat(disconnect.detail()).contains("database down");
		assertThat(received).extracting(ServerSentEvent::data).containsExactly("a");
	}

	@Test
	void interruptingTheReaderEndsTheConnection() throws InterruptedException {
		server.then(eventsThenSilence("data: a\n\n"));
		EventStreamClient patient = new EventStreamClient(server.url(), Duration.ofSeconds(5), Duration.ofMinutes(1));
		List<Disconnect> result = new ArrayList<>();

		Thread reader = Thread.ofPlatform().start(() -> result.add(patient.read("", received::add)));
		Thread.sleep(300);
		reader.interrupt();
		reader.join(Duration.ofSeconds(5));

		assertThat(reader.isAlive()).isFalse();
		assertThat(result).singleElement().extracting(Disconnect::reason).isEqualTo(Reason.NETWORK_ERROR);
	}

	private static void sleep(Duration duration) {
		try {
			Thread.sleep(duration);
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
	}

}
