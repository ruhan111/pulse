package com.example.pulse;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The whole application, end to end: a local feed server, the real scheduler, fetcher, parser,
 * mapper and the real {@code EventSink}. Polls every 100ms so several rounds run during the test.
 */
@SpringBootTest
@ExtendWith(OutputCaptureExtension.class)
class RssIngestionEndToEndTest {

	private static final HttpServer FEED_SERVER = startFeedServer();
	private static final AtomicInteger REQUESTS = new AtomicInteger();

	@DynamicPropertySource
	static void pollLocalFeed(DynamicPropertyRegistry registry) {
		registry.add("pulse.rss.feeds[0]", () -> "http://127.0.0.1:" + FEED_SERVER.getAddress().getPort() + "/feed");
		registry.add("pulse.rss.poll-interval", () -> "100ms");
	}

	@AfterAll
	static void stopFeedServer() {
		FEED_SERVER.stop(0);
	}

	@Test
	void publishesEachItemOnceAcrossRepeatedPolls(CapturedOutput output) throws InterruptedException {
		long deadline = System.currentTimeMillis() + 10_000;
		while (REQUESTS.get() < 3 && System.currentTimeMillis() < deadline) {
			Thread.sleep(50);
		}
		Thread.sleep(200); // let the last round finish publishing

		assertThat(REQUESTS.get()).isGreaterThanOrEqualTo(3);
		// rss2.xml has 4 valid items; re-polls publish them again, but the sink logs each id once.
		assertThat(output.getOut().lines().filter(line -> line.contains("new event RSS"))).hasSize(4);
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
