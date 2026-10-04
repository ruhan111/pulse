package com.example.pulse.ingestion.wikipedia;

import com.example.pulse.ingestion.wikipedia.Disconnect.Reason;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Reads a server-sent event stream over one HTTP connection until it ends, handing each event to a
 * handler on the calling thread. Reconnecting is the caller's job: every call is one connection.
 * <p>
 * The request timeout only covers waiting for the response headers. Once the body is flowing,
 * {@code HttpClient} has no read timeout, so a connection that silently dies would block forever.
 * A watchdog closes the body when no line has arrived for {@code idleTimeout}, which makes the
 * blocked read fail. Time spent in the handler doesn't count: a slow database is not a dead
 * connection.
 */
class EventStreamClient {

	static final String USER_AGENT = "Pulse/0.1 (+https://github.com/ruhan111/pulse)";

	private final HttpClient client;
	private final URI url;
	private final Duration connectTimeout;
	private final Duration idleTimeout;

	/** One daemon thread for all connections of this client; it only ever closes a stream. */
	private final ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor(
			Thread.ofPlatform().name("event-stream-watchdog").daemon().factory());

	EventStreamClient(URI url, Duration connectTimeout, Duration idleTimeout) {
		this.url = requireNonNull(url, "url");
		this.connectTimeout = requireNonNull(connectTimeout, "connectTimeout");
		this.idleTimeout = requireNonNull(idleTimeout, "idleTimeout");
		this.client = HttpClient.newBuilder()
			.connectTimeout(connectTimeout)
			// Honors the standard JVM proxy settings (-Dhttps.proxyHost…); without them it connects directly.
			.proxy(ProxySelector.getDefault())
			.followRedirects(HttpClient.Redirect.NORMAL)
			.build();
	}

	/**
	 * Connects, resuming after {@code lastEventId} unless it is empty, and reads until the stream
	 * ends. If the handler throws, reading stops at once and that event counts as not handled.
	 */
	Disconnect read(String lastEventId, Consumer<ServerSentEvent> handler) {
		HttpResponse<InputStream> response;
		try {
			response = client.send(request(lastEventId), BodyHandlers.ofInputStream());
		}
		catch (IOException ex) {
			return new Disconnect(Reason.NETWORK_ERROR, ex.toString());
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			return new Disconnect(Reason.NETWORK_ERROR, "interrupted");
		}

		try (InputStream body = response.body()) {
			if (response.statusCode() != 200) {
				return response.statusCode() == 400 && !lastEventId.isEmpty()
						? new Disconnect(Reason.REJECTED_POSITION, "status 400 for Last-Event-ID " + lastEventId)
						: new Disconnect(Reason.HTTP_ERROR, "status " + response.statusCode());
			}
			return readEvents(body, handler);
		}
		catch (IOException ex) {
			// Only closing a body we no longer need can fail here.
			return new Disconnect(Reason.NETWORK_ERROR, ex.toString());
		}
	}

	private HttpRequest request(String lastEventId) {
		HttpRequest.Builder builder = HttpRequest.newBuilder(url)
			.GET()
			.timeout(connectTimeout)
			// Wikimedia answers 403 without a User-Agent.
			.header("User-Agent", USER_AGENT)
			.header("Accept", "text/event-stream");
		if (!lastEventId.isEmpty()) {
			builder.header("Last-Event-ID", lastEventId);
		}
		return builder.build();
	}

	private Disconnect readEvents(InputStream body, Consumer<ServerSentEvent> handler) {
		Activity activity = new Activity();
		long period = Math.max(1, idleTimeout.toMillis() / 4);
		ScheduledFuture<?> check = watchdog.scheduleAtFixedRate(() -> activity.closeIfIdle(body, idleTimeout),
				period, period, TimeUnit.MILLISECONDS);
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(body, UTF_8));
			SseParser parser = new SseParser();
			String line;
			while ((line = reader.readLine()) != null) {
				activity.lineReceived();
				Optional<ServerSentEvent> event = parser.line(line);
				if (event.isPresent()) {
					activity.handling(true);
					try {
						handler.accept(event.get());
					}
					catch (RuntimeException ex) {
						return new Disconnect(Reason.HANDLER_FAILED, ex.toString());
					}
					finally {
						activity.handling(false);
					}
				}
			}
			return new Disconnect(Reason.ENDED, "server closed the stream");
		}
		catch (IOException ex) {
			return activity.timedOut()
					? new Disconnect(Reason.IDLE_TIMEOUT, "nothing received for " + idleTimeout)
					: new Disconnect(Reason.NETWORK_ERROR, ex.toString());
		}
		finally {
			check.cancel(false);
		}
	}

	/** Shared between the reading thread and the watchdog. */
	private static final class Activity {

		private volatile long lastLine = System.nanoTime();
		private volatile boolean handling;
		private volatile boolean timedOut;

		void lineReceived() {
			lastLine = System.nanoTime();
		}

		void handling(boolean handling) {
			this.handling = handling;
			lastLine = System.nanoTime();
		}

		boolean timedOut() {
			return timedOut;
		}

		void closeIfIdle(InputStream body, Duration idleTimeout) {
			if (!handling && !timedOut && System.nanoTime() - lastLine > idleTimeout.toNanos()) {
				timedOut = true;
				try {
					// Makes the blocked read on the other thread fail with an IOException.
					body.close();
				}
				catch (IOException ignored) {
					// Closing is all we wanted; the reader sees the failure either way.
				}
			}
		}

	}

}
