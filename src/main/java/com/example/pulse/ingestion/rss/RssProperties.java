package com.example.pulse.ingestion.rss;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;

import java.net.URI;
import java.time.Duration;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * RSS ingestion settings under {@code pulse.rss}. The values live in {@code application.yaml}; this
 * record is the typed contract for them, so a missing or malformed setting stops the app at
 * startup instead of failing on the first poll.
 *
 * @param enabled       whether to poll at all; off when not set
 * @param feeds         feed URLs to poll; empty when not set
 * @param pollInterval  pause between the end of one round of polls and the start of the next
 * @param fetchDeadline maximum time for one complete fetch, body included
 * @param maxFeedSize   larger feeds are rejected while downloading
 */
@ConfigurationProperties("pulse.rss")
public record RssProperties(
		boolean enabled,
		List<URI> feeds,
		Duration pollInterval,
		Duration fetchDeadline,
		DataSize maxFeedSize) {

	public RssProperties {
		feeds = feeds == null ? List.of() : List.copyOf(feeds);
		requireNonNull(pollInterval, "pulse.rss.poll-interval must be set");
		requireNonNull(fetchDeadline, "pulse.rss.fetch-deadline must be set");
		requireNonNull(maxFeedSize, "pulse.rss.max-feed-size must be set");
		if (maxFeedSize.toBytes() > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("pulse.rss.max-feed-size must be below 2GB");
		}
	}

}
