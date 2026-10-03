package com.example.pulse.ingestion.rss;

import com.example.pulse.ingestion.rss.FetchResult.Failed;
import com.example.pulse.ingestion.rss.FetchResult.Fetched;
import com.example.pulse.ingestion.rss.FetchResult.NotModified;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

class HttpFeedFetcherTest {

	private static final byte[] FEED = "<rss version=\"2.0\"><channel><title>t</title></channel></rss>"
		.getBytes(StandardCharsets.UTF_8);

	private final HttpFeedFetcher fetcher = new HttpFeedFetcher(Duration.ofMillis(500), 1_000);
	// Header names are case-insensitive, and the JDK server normalizes them ("User-agent").
	private final Map<String, String> receivedHeaders =
			Collections.synchronizedMap(new TreeMap<>(String.CASE_INSENSITIVE_ORDER));
	private HttpServer server;

	@BeforeEach
	void startServer() throws IOException {
		server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
		server.setExecutor(Executors.newCachedThreadPool());
		server.start();
	}

	@AfterEach
	void stopServer() {
		server.stop(0);
	}

	@Test
	void returnsBodyAndValidators() {
		serve(exchange -> {
			exchange.getResponseHeaders().add("ETag", "\"v1\"");
			exchange.getResponseHeaders().add("Last-Modified", "Wed, 30 Sep 2026 10:00:00 GMT");
			respond(exchange, 200, FEED);
		});

		Fetched fetched = (Fetched) fetcher.fetch(url(), CacheValidators.NONE);

		assertThat(fetched.body()).isEqualTo(FEED);
		assertThat(fetched.validators()).isEqualTo(new CacheValidators("\"v1\"", "Wed, 30 Sep 2026 10:00:00 GMT"));
	}

	@Test
	void identifiesItselfToServer() {
		serve(exchange -> respond(exchange, 200, FEED));

		fetcher.fetch(url(), CacheValidators.NONE);

		assertThat(receivedHeaders).containsEntry("User-Agent", HttpFeedFetcher.USER_AGENT);
	}

	@Test
	void sendsValidatorsBackAndHandlesNotModified() {
		serve(exchange -> respond(exchange, 304, new byte[0]));

		FetchResult result = fetcher.fetch(url(), new CacheValidators("\"v1\"", "Wed, 30 Sep 2026 10:00:00 GMT"));

		assertThat(result).isInstanceOf(NotModified.class);
		assertThat(receivedHeaders)
			.containsEntry("If-None-Match", "\"v1\"")
			.containsEntry("If-Modified-Since", "Wed, 30 Sep 2026 10:00:00 GMT");
	}

	@Test
	void doesNotSendEmptyValidators() {
		serve(exchange -> respond(exchange, 200, FEED));

		fetcher.fetch(url(), CacheValidators.NONE);

		assertThat(receivedHeaders).doesNotContainKeys("If-None-Match", "If-Modified-Since");
	}

	@Test
	void reportsServerErrors() {
		serve(exchange -> respond(exchange, 503, "down".getBytes(StandardCharsets.UTF_8)));

		assertThat(failure()).isEqualTo(new Failed(FetchFailure.HTTP_ERROR, "status 503"));
	}

	@Test
	void followsRedirects() {
		server.createContext("/old", exchange -> {
			exchange.getResponseHeaders().add("Location", "/feed");
			respond(exchange, 301, new byte[0]);
		});
		serve(exchange -> respond(exchange, 200, FEED));

		FetchResult result = fetcher.fetch(url("/old"), CacheValidators.NONE);

		assertThat(result).isInstanceOf(Fetched.class);
	}

	@Test
	void timesOutWhenServerIsSlowToRespond() {
		serve(exchange -> {
			sleep(2_000);
			respond(exchange, 200, FEED);
		});

		assertThat(failure().reason()).isEqualTo(FetchFailure.TIMEOUT);
	}

	@Test
	void timesOutWhenServerSendsHeadersButDripsTheBody() {
		serve(exchange -> {
			exchange.sendResponseHeaders(200, 0);
			try (OutputStream body = exchange.getResponseBody()) {
				for (int i = 0; i < 20; i++) {
					body.write('x');
					body.flush();
					sleep(100);
				}
			}
		});

		assertThat(failure().reason()).isEqualTo(FetchFailure.TIMEOUT);
	}

	@Test
	void rejectsBodyLargerThanLimit() {
		serve(exchange -> respond(exchange, 200, new byte[5_000]));

		assertThat(failure().reason()).isEqualTo(FetchFailure.TOO_LARGE);
	}

	@Test
	void reportsConnectionFailure() throws IOException {
		int closedPort;
		try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
			closedPort = socket.getLocalPort();
		}

		FetchResult result = fetcher.fetch(URI.create("http://127.0.0.1:" + closedPort + "/feed"), CacheValidators.NONE);

		assertThat(((Failed) result).reason()).isEqualTo(FetchFailure.NETWORK_ERROR);
	}

	private void serve(HttpHandler handler) {
		server.createContext("/feed", exchange -> {
			exchange.getRequestHeaders().forEach((name, values) -> receivedHeaders.put(name, values.getFirst()));
			handler.handle(exchange);
		});
	}

	private Failed failure() {
		FetchResult result = fetcher.fetch(url(), CacheValidators.NONE);
		assertThat(result).isInstanceOf(Failed.class);
		return (Failed) result;
	}

	private URI url() {
		return url("/feed");
	}

	private URI url(String path) {
		return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + path);
	}

	private static void respond(HttpExchange exchange, int status, byte[] body) throws IOException {
		exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
		try (OutputStream out = exchange.getResponseBody()) {
			out.write(body);
		}
	}

	private static void sleep(long millis) {
		try {
			Thread.sleep(millis);
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
	}

}
