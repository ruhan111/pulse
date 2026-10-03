package com.example.pulse.ingestion.rss;

import java.net.URI;

/**
 * Gets the current version of a feed. {@link HttpFeedFetcher} does it over HTTP; tests substitute
 * a fake so the poller can be tested without a network.
 */
@FunctionalInterface
interface FeedFetcher {

	FetchResult fetch(URI url, CacheValidators previous);

}
