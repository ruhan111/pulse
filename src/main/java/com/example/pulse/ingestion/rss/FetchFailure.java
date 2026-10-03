package com.example.pulse.ingestion.rss;

/** Why fetching a feed failed. Countable, so it can become a metric. */
enum FetchFailure {

	/** The whole fetch, including downloading the body, took longer than the deadline. */
	TIMEOUT,

	/** Connection refused, DNS failure, reset connection… */
	NETWORK_ERROR,

	/** The server answered with a status other than 200 or 304. */
	HTTP_ERROR,

	/** The body was larger than the configured limit. */
	TOO_LARGE

}
