package com.example.pulse.ingestion.wikipedia;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A local event-stream server whose responses are scripted per request, in order. Records the
 * headers of every request. Handlers run on a thread pool: the JDK server's default executor is a
 * single thread, so one stalled stream would block every later request.
 */
final class StreamServer implements AutoCloseable {

	/** What to do with one request. */
	@FunctionalInterface
	interface Response {

		void send(HttpExchange exchange) throws IOException, InterruptedException;

	}

	private final HttpServer server;
	private final Queue<Response> responses = new ConcurrentLinkedQueue<>();
	private final List<Headers> requests = new CopyOnWriteArrayList<>();

	StreamServer() throws IOException {
		server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.setExecutor(Executors.newCachedThreadPool());
		server.createContext("/stream", exchange -> {
			requests.add(exchange.getRequestHeaders());
			Response response = responses.poll();
			try {
				if (response == null) {
					exchange.sendResponseHeaders(503, -1);
				}
				else {
					response.send(exchange);
				}
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
			finally {
				exchange.close();
			}
		});
		server.start();
	}

	URI url() {
		return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/stream");
	}

	StreamServer then(Response response) {
		responses.add(response);
		return this;
	}

	List<Headers> requests() {
		return requests;
	}

	/** Sends the text as an event stream and ends the response. */
	static Response events(String text) {
		return exchange -> {
			OutputStream body = start(exchange);
			body.write(text.getBytes(UTF_8));
		};
	}

	/** Sends the text, then keeps the connection open without sending anything. */
	static Response eventsThenSilence(String text) {
		return exchange -> {
			OutputStream body = start(exchange);
			body.write(text.getBytes(UTF_8));
			body.flush();
			Thread.sleep(60_000);
		};
	}

	static Response status(int status) {
		return exchange -> exchange.sendResponseHeaders(status, -1);
	}

	private static OutputStream start(HttpExchange exchange) throws IOException {
		exchange.getResponseHeaders().add("Content-Type", "text/event-stream; charset=utf-8");
		exchange.sendResponseHeaders(200, 0);
		return exchange.getResponseBody();
	}

	@Override
	public void close() {
		server.stop(0);
	}

}
