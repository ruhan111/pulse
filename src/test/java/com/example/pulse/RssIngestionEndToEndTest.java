package com.example.pulse;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The whole application, end to end: a local feed server, the real scheduler, fetcher, parser,
 * mapper, {@code EventSink} and PostgreSQL. Polls every 100ms so several rounds run per start.
 * <p>
 * Starts the application itself, twice, to show that a restart remembers what was stored.
 */
@ExtendWith(OutputCaptureExtension.class)
class RssIngestionEndToEndTest {

	private static final HttpServer FEED_SERVER = startFeedServer();
	private static final AtomicInteger REQUESTS = new AtomicInteger();
	private static final String FEED = "http://127.0.0.1:" + FEED_SERVER.getAddress().getPort() + "/feed";

	@AfterAll
	static void stopFeedServer() {
		FEED_SERVER.stop(0);
	}

	@Test
	void storesEachItemOnceAcrossRepeatedPollsAndRestarts(CapturedOutput output) throws Exception {
		try (ConfigurableApplicationContext app = startApplication()) {
			awaitPolls(3);
			// rss2.xml has 4 valid items; re-polls publish them again, but they are stored once.
			assertThat(countStoredEvents(app)).isEqualTo(4);
		}
		String firstRun = output.getOut();
		assertThat(newEventLines(firstRun)).isEqualTo(4);
		assertThat(firstRun).contains("new=4 duplicates=0").contains("new=0 duplicates=4");

		try (ConfigurableApplicationContext restarted = startApplication()) {
			awaitPolls(2);
			assertThat(countStoredEvents(restarted)).isEqualTo(4);
		}
		String afterRestart = output.getOut().substring(firstRun.length());
		// Before Phase 3 a restart forgot every id and reported all 4 items as new again.
		assertThat(newEventLines(afterRestart)).isZero();
		assertThat(afterRestart).contains("new=0 duplicates=4").doesNotContain("new=4");
	}

	private static ConfigurableApplicationContext startApplication() {
		Map<String, Object> properties = new HashMap<>(TestDatabase.properties());
		properties.put("pulse.rss.feeds[0]", FEED);
		properties.put("pulse.rss.poll-interval", "100ms");
		// Command-line arguments, because builder properties are only defaults and application.yaml wins.
		String[] args = properties.entrySet().stream()
			.map(property -> "--" + property.getKey() + "=" + property.getValue())
			.toArray(String[]::new);
		return new SpringApplicationBuilder(PulseApplication.class).run(args);
	}

	/** Waits for this many more feed requests, then lets the last round finish publishing. */
	private static void awaitPolls(int polls) throws InterruptedException {
		int target = REQUESTS.get() + polls;
		long deadline = System.currentTimeMillis() + 10_000;
		while (REQUESTS.get() < target && System.currentTimeMillis() < deadline) {
			Thread.sleep(50);
		}
		Thread.sleep(200);
		assertThat(REQUESTS.get()).isGreaterThanOrEqualTo(target);
	}

	private static int countStoredEvents(ConfigurableApplicationContext app) throws IOException {
		String sql = new ClassPathResource("sql/count_events_in_channel.sql").getContentAsString(StandardCharsets.UTF_8);
		return app.getBean(JdbcClient.class).sql(sql).param("channel", FEED).query(Integer.class).single();
	}

	private static long newEventLines(String log) {
		return log.lines().filter(line -> line.contains("new event RSS")).count();
	}

	private static HttpServer startFeedServer() {
		try (InputStream sample = RssIngestionEndToEndTest.class.getResourceAsStream("/rss/rss2.xml")) {
			byte[] feed = sample.readAllBytes();
			HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
			server.createContext("/feed", exchange -> {
				REQUESTS.incrementAndGet();
				exchange.sendResponseHeaders(200, feed.length);
				try (OutputStream out = exchange.getResponseBody()) {
					out.write(feed);
				}
			});
			server.start();
			return server;
		}
		catch (IOException ex) {
			throw new UncheckedIOException(ex);
		}
	}

}
