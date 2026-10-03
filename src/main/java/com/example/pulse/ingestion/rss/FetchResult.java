package com.example.pulse.ingestion.rss;

import static java.util.Objects.requireNonNull;

/**
 * The outcome of fetching a feed. Failing is an expected result, not an exception: at the network
 * boundary things go wrong all the time, and the caller should handle every case explicitly.
 */
sealed interface FetchResult {

	/** The server sent the feed. {@code validators} should be sent back on the next fetch. */
	record Fetched(byte[] body, CacheValidators validators) implements FetchResult {

		public Fetched {
			requireNonNull(body, "body");
			requireNonNull(validators, "validators");
		}

	}

	/** The feed hasn't changed since the version described by the validators we sent. */
	record NotModified() implements FetchResult {
	}

	record Failed(FetchFailure reason, String detail) implements FetchResult {

		public Failed {
			requireNonNull(reason, "reason");
			detail = detail == null ? "" : detail;
		}

	}

}
