package com.example.pulse.ingestion.rss;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.util.unit.DataSize;

import java.net.URI;
import java.time.Duration;
import java.util.List;

/**
 * RSS ingestion settings under {@code pulse.rss}. Adding a feed or changing a limit is
 * configuration, not code.
 *
 * @param enabled       off by default, so the app can run without polling (e.g. during load tests)
 * @param feeds         feed URLs to poll
 * @param pollInterval  pause between the end of one round of polls and the start of the next
 * @param fetchDeadline maximum time for one complete fetch, body included
 * @param maxFeedSize   larger feeds are rejected while downloading
 */
@ConfigurationProperties("pulse.rss")
public record RssProperties(
		@DefaultValue("false") boolean enabled,
		@DefaultValue List<URI> feeds,
		@DefaultValue("5m") Duration pollInterval,
		@DefaultValue("10s") Duration fetchDeadline,
		@DefaultValue("5MB") DataSize maxFeedSize) {

	public RssProperties {
		feeds = List.copyOf(feeds);
		if (maxFeedSize.toBytes() > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("maxFeedSize must be below 2GB");
		}
	}

}
