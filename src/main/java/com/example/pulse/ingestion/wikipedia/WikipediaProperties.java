package com.example.pulse.ingestion.wikipedia;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.net.URI;
import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * Wikipedia ingestion settings under {@code pulse.wikipedia}. Like {@code RssProperties}, a missing
 * setting stops the app at startup instead of failing on the first connection.
 *
 * @param enabled            whether to read the stream at all; off when not set
 * @param streamUrl          Wikimedia's recentchange EventStreams URL
 * @param connectTimeout     maximum time to connect and receive the response headers
 * @param idleTimeout        a connection silent for this long is treated as dead and replaced
 * @param checkpointInterval how often the resume position is saved while reading
 */
@ConfigurationProperties("pulse.wikipedia")
public record WikipediaProperties(
		boolean enabled,
		URI streamUrl,
		Duration connectTimeout,
		Duration idleTimeout,
		Duration checkpointInterval) {

	public WikipediaProperties {
		requireNonNull(streamUrl, "pulse.wikipedia.stream-url must be set");
		requireNonNull(connectTimeout, "pulse.wikipedia.connect-timeout must be set");
		requireNonNull(idleTimeout, "pulse.wikipedia.idle-timeout must be set");
		requireNonNull(checkpointInterval, "pulse.wikipedia.checkpoint-interval must be set");
	}

}
