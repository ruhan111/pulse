package com.example.pulse.ingestion.wikipedia;

import com.example.pulse.PulseApplication;
import com.example.pulse.TestDatabase;
import com.example.pulse.event.EventId;
import com.example.pulse.event.Source;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.example.pulse.ingestion.wikipedia.StreamServer.eventsThenSilence;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The whole application with the Wikipedia stream: a local stream server replaying the real
 * capture, the Spring wiring and lifecycle, the mapper, the sink, the checkpoint store and
 * PostgreSQL. Starts the application twice, to show that shutdown saves the position and a restart
 * resumes from it.
 */
class WikipediaIngestionEndToEndTest {

	private static final String CAPTURE = RecentChangeMapperTest.resource("recentchange-stream.txt");
	/** The capture's two kept changes: a new page and an edit of it. */
	private static final List<String> KEPT = List.of("enwiki:1378416740", "enwiki:1378416745");

	private final StreamServer server = new StreamServer();

	WikipediaIngestionEndToEndTest() throws IOException {
	}

	@AfterEach
	void stopServer() {
		server.close();
	}

	@Test
	void storesKeptChangesOnceAndResumesAfterARestart() throws Exception {
		// The stream stays open after the capture, like the real one between changes.
		server.then(eventsThenSilence(CAPTURE)).then(eventsThenSilence(CAPTURE));

		try (ConfigurableApplicationContext app = startApplication()) {
			awaitStored(app, KEPT.size());
			assertThat(get(app, "/actuator/metrics/pulse.wikipedia.changes.received"))
				.contains("\"statistic\":\"COUNT\",\"value\":120.0");
			assertThat(get(app, "/actuator/metrics/pulse.events.accepted?tag=source:WIKIPEDIA&tag=result:NEW"))
				.contains("\"statistic\":\"COUNT\",\"value\":2.0");
			// Registered at 0, so a reason that hasn't happened yet is not a 404.
			assertThat(get(app, "/actuator/metrics/pulse.wikipedia.changes.skipped?tag=reason:MALFORMED"))
				.contains("\"value\":0.0");
		}
		String lastId = CAPTURE.lines().filter(line -> line.startsWith("id: ")).toList().getLast().substring(4);
		assertThat(server.requests().getFirst().containsKey("Last-Event-ID")).isFalse();

		try (ConfigurableApplicationContext restarted = startApplication()) {
			awaitRequests(2);
			// Shutdown saved the position, and the restart resumed 5 s before it.
			assertThat(server.requests().get(1).getFirst("Last-Event-ID"))
				.isEqualTo(ResumePosition.rewind(lastId, RecentChangeConsumer.REPLAY_MARGIN));
			// This server replays everything; the repeats are duplicates, not new rows.
			awaitDuplicates(restarted, KEPT.size());
			assertThat(storedCount(restarted)).isEqualTo(KEPT.size());
		}
	}

	private ConfigurableApplicationContext startApplication() {
		Map<String, Object> properties = new HashMap<>(TestDatabase.properties());
		properties.put("pulse.rss.enabled", "false");
		properties.put("pulse.wikipedia.stream-url", server.url());
		properties.put("server.port", "0");
		// Command-line arguments, because builder properties are only defaults and application.yaml wins.
		String[] args = properties.entrySet().stream()
			.map(property -> "--" + property.getKey() + "=" + property.getValue())
			.toArray(String[]::new);
		return new SpringApplicationBuilder(PulseApplication.class).run(args);
	}

	private static void awaitStored(ConfigurableApplicationContext app, int count) throws Exception {
		long deadline = System.currentTimeMillis() + 10_000;
		while (storedCount(app) < count && System.currentTimeMillis() < deadline) {
			Thread.sleep(50);
		}
		assertThat(storedCount(app)).isEqualTo(count);
		// The remaining skipped changes of the capture follow the last kept one.
		Thread.sleep(300);
	}

	private void awaitRequests(int count) throws InterruptedException {
		long deadline = System.currentTimeMillis() + 10_000;
		while (server.requests().size() < count && System.currentTimeMillis() < deadline) {
			Thread.sleep(50);
		}
		assertThat(server.requests()).hasSize(count);
	}

	private static void awaitDuplicates(ConfigurableApplicationContext app, int count) throws Exception {
		String path = "/actuator/metrics/pulse.events.accepted?tag=source:WIKIPEDIA&tag=result:DUPLICATE";
		String expected = "\"statistic\":\"COUNT\",\"value\":" + count + ".0";
		long deadline = System.currentTimeMillis() + 10_000;
		while (!request(app, path).body().contains(expected) && System.currentTimeMillis() < deadline) {
			Thread.sleep(50);
		}
		assertThat(get(app, path)).contains(expected);
	}

	private static int storedCount(ConfigurableApplicationContext app) throws IOException {
		String sql = new ClassPathResource("sql/count_events_with_id.sql").getContentAsString(StandardCharsets.UTF_8);
		JdbcClient jdbc = app.getBean(JdbcClient.class);
		int count = 0;
		for (String externalId : KEPT) {
			count += jdbc.sql(sql).param("id", EventId.of(Source.WIKIPEDIA, externalId).value()).query(Integer.class)
				.single();
		}
		return count;
	}

	private static String get(ConfigurableApplicationContext app, String path) throws Exception {
		HttpResponse<String> response = request(app, path);
		assertThat(response.statusCode()).as(path).isEqualTo(200);
		return response.body();
	}

	private static HttpResponse<String> request(ConfigurableApplicationContext app, String path) throws Exception {
		String port = app.getEnvironment().getProperty("local.server.port");
		try (HttpClient client = HttpClient.newHttpClient()) {
			HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).build();
			return client.send(request, HttpResponse.BodyHandlers.ofString());
		}
	}

}
