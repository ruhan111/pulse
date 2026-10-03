package com.example.pulse.ingestion.rss;

import com.example.pulse.ingestion.rss.FetchResult.Failed;
import com.example.pulse.ingestion.rss.FetchResult.Fetched;
import com.example.pulse.ingestion.rss.FetchResult.NotModified;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodySubscriber;
import java.net.http.HttpResponse.BodySubscribers;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;

/**
 * Downloads a feed over HTTP. Stateless: the caller keeps the {@link CacheValidators} from the last
 * fetch and passes them in, which keeps this class trivial to test and makes it explicit where
 * per-feed state lives.
 * <p>
 * Two limits protect the rest of the system from a misbehaving server:
 * <ul>
 * <li>a <b>deadline</b> for the whole fetch, including the body. A per-request timeout alone only
 * covers waiting for headers, so a server dripping one byte per second would hang the caller.</li>
 * <li>a <b>size limit</b>. The body is read chunk by chunk and the download is cancelled as soon as
 * the limit is exceeded, so memory use is bounded no matter what the server sends.</li>
 * </ul>
 */
public class FeedFetcher {

	static final String USER_AGENT = "Pulse/0.1 (+https://github.com/ruhan111/pulse)";

	private static final String ACCEPT =
			"application/rss+xml, application/atom+xml, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.5";

	private final HttpClient client;
	private final Duration deadline;
	private final int maxBytes;

	public FeedFetcher(Duration deadline, int maxBytes) {
		this.deadline = requireNonNull(deadline, "deadline");
		this.maxBytes = maxBytes;
		this.client = HttpClient.newBuilder()
			.connectTimeout(deadline)
			// Follows 301/302/303/307/308, but never from https to http.
			.followRedirects(HttpClient.Redirect.NORMAL)
			.build();
	}

	public FetchResult fetch(URI url, CacheValidators previous) {
		HttpRequest request = request(url, previous);
		CompletableFuture<HttpResponse<byte[]>> response = client.sendAsync(request, this::bodyFor);
		try {
			return toResult(response.get(deadline.toMillis(), TimeUnit.MILLISECONDS));
		}
		catch (TimeoutException ex) {
			// Aborts the exchange, so a slow server can't keep a connection open in the background.
			response.cancel(true);
			return new Failed(FetchFailure.TIMEOUT, "no complete response within " + deadline);
		}
		catch (ExecutionException ex) {
			return failure(ex.getCause());
		}
		catch (InterruptedException ex) {
			response.cancel(true);
			Thread.currentThread().interrupt();
			return new Failed(FetchFailure.NETWORK_ERROR, "interrupted");
		}
	}

	private HttpRequest request(URI url, CacheValidators previous) {
		HttpRequest.Builder builder = HttpRequest.newBuilder(url)
			.GET()
			.timeout(deadline)
			.header("User-Agent", USER_AGENT)
			.header("Accept", ACCEPT);
		if (!previous.etag().isEmpty()) {
			builder.header("If-None-Match", previous.etag());
		}
		if (!previous.lastModified().isEmpty()) {
			builder.header("If-Modified-Since", previous.lastModified());
		}
		return builder.build();
	}

	/** Only a 200 body is worth reading; anything else is discarded without buffering it. */
	private BodySubscriber<byte[]> bodyFor(HttpResponse.ResponseInfo info) {
		return info.statusCode() == 200
				? new LimitedBodySubscriber(maxBytes)
				: BodySubscribers.replacing(new byte[0]);
	}

	private static FetchResult toResult(HttpResponse<byte[]> response) {
		return switch (response.statusCode()) {
			case 200 -> new Fetched(response.body(), new CacheValidators(
					response.headers().firstValue("ETag").orElse(""),
					response.headers().firstValue("Last-Modified").orElse("")));
			case 304 -> new NotModified();
			default -> new Failed(FetchFailure.HTTP_ERROR, "status " + response.statusCode());
		};
	}

	private static FetchResult failure(Throwable cause) {
		return switch (cause) {
			case BodyTooLargeException tooLarge -> new Failed(FetchFailure.TOO_LARGE, tooLarge.getMessage());
			case HttpTimeoutException timeout -> new Failed(FetchFailure.TIMEOUT, timeout.getMessage());
			case IOException io -> new Failed(FetchFailure.NETWORK_ERROR, io.toString());
			default -> new Failed(FetchFailure.NETWORK_ERROR, String.valueOf(cause));
		};
	}

	/**
	 * Collects the body one chunk at a time and cancels the download once it exceeds the limit.
	 * Requesting one chunk at a time is backpressure in its simplest form: the server can only send
	 * as fast as we consume.
	 */
	private static final class LimitedBodySubscriber implements BodySubscriber<byte[]> {

		private final int maxBytes;
		private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
		private final CompletableFuture<byte[]> body = new CompletableFuture<>();
		private Flow.Subscription subscription;

		LimitedBodySubscriber(int maxBytes) {
			this.maxBytes = maxBytes;
		}

		@Override
		public void onSubscribe(Flow.Subscription subscription) {
			this.subscription = subscription;
			subscription.request(1);
		}

		@Override
		public void onNext(List<ByteBuffer> chunks) {
			for (ByteBuffer chunk : chunks) {
				if (buffer.size() + chunk.remaining() > maxBytes) {
					subscription.cancel();
					body.completeExceptionally(new BodyTooLargeException(maxBytes));
					return;
				}
				byte[] bytes = new byte[chunk.remaining()];
				chunk.get(bytes);
				buffer.writeBytes(bytes);
			}
			subscription.request(1);
		}

		@Override
		public void onError(Throwable error) {
			body.completeExceptionally(error);
		}

		@Override
		public void onComplete() {
			body.complete(buffer.toByteArray());
		}

		@Override
		public CompletableFuture<byte[]> getBody() {
			return body;
		}

	}

	private static final class BodyTooLargeException extends IOException {

		BodyTooLargeException(int maxBytes) {
			super("body larger than " + maxBytes + " bytes");
		}

	}

}
