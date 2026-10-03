package com.example.pulse.ingestion.rss;

import com.example.pulse.event.EventSink;
import com.example.pulse.event.EventSink.Accepted;
import com.example.pulse.ingestion.rss.FetchResult.Failed;
import com.example.pulse.ingestion.rss.FetchResult.Fetched;
import com.example.pulse.ingestion.rss.FetchResult.NotModified;
import com.example.pulse.ingestion.rss.MappedEntry.Mapped;
import com.example.pulse.ingestion.rss.MappedEntry.Skipped;
import com.example.pulse.ingestion.rss.PollResult.Outcome;
import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndFeed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.time.Duration;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * Polls every configured feed: fetch, parse, map, publish.
 * <p>
 * Failures are isolated at every level: a bad entry is skipped, a broken feed is logged, and the
 * remaining feeds are still polled.
 * <p>
 * Cache validators are only stored once every event from a fetch has been published. If the sink
 * fails halfway, the next poll fetches the full feed again instead of getting a 304 and silently
 * losing the unpublished events. Already-published events are then sent a second time, which is
 * safe because sinks must be idempotent on {@code EventId}. This is at-least-once delivery.
 */
class RssPoller {

	private static final Logger log = LoggerFactory.getLogger(RssPoller.class);

	private final List<URI> feeds;
	private final FeedFetcher fetcher;
	private final RssFeedParser parser;
	private final RssEventMapper mapper;
	private final EventSink sink;

	/** Per-feed state, in memory: lost on restart, which only costs one full fetch per feed. */
	private final Map<URI, CacheValidators> validators = new ConcurrentHashMap<>();

	RssPoller(List<URI> feeds, FeedFetcher fetcher, RssFeedParser parser, RssEventMapper mapper, EventSink sink) {
		this.feeds = List.copyOf(feeds);
		this.fetcher = requireNonNull(fetcher, "fetcher");
		this.parser = requireNonNull(parser, "parser");
		this.mapper = requireNonNull(mapper, "mapper");
		this.sink = requireNonNull(sink, "sink");
	}

	void pollAll() {
		for (URI feed : feeds) {
			try {
				log(poll(feed));
			}
			catch (RuntimeException ex) {
				// A bug in one feed's handling must not stop the others.
				log.error("rss poll {} failed unexpectedly", feed, ex);
			}
		}
	}

	PollResult poll(URI feed) {
		long start = System.nanoTime();
		FetchResult fetched = fetcher.fetch(feed, validators.getOrDefault(feed, CacheValidators.NONE));
		return switch (fetched) {
			case NotModified notModified -> result(feed, Outcome.NOT_MODIFIED, new Counts(), Map.of(), "", start);
			case Failed failed -> result(feed, Outcome.FETCH_FAILED, new Counts(), Map.of(),
					failed.reason() + ": " + failed.detail(), start);
			case Fetched body -> publish(feed, body, start);
		};
	}

	private PollResult publish(URI feed, Fetched fetched, long start) {
		SyndFeed parsed;
		try {
			parsed = parser.parse(new ByteArrayInputStream(fetched.body()));
		}
		catch (FeedParseException ex) {
			return result(feed, Outcome.PARSE_FAILED, new Counts(), Map.of(), rootMessage(ex), start);
		}

		Counts counts = new Counts();
		Map<SkipReason, Integer> skipped = new EnumMap<>(SkipReason.class);
		for (SyndEntry entry : parsed.getEntries()) {
			switch (mapper.map(entry, feed)) {
				case Skipped skip -> skipped.merge(skip.reason(), 1, Integer::sum);
				case Mapped mapped -> {
					try {
						counts.add(sink.accept(mapped.event()));
					}
					catch (RuntimeException ex) {
						return result(feed, Outcome.PUBLISH_FAILED, counts, skipped, ex.toString(), start);
					}
				}
			}
		}
		validators.put(feed, fetched.validators());
		return result(feed, Outcome.PUBLISHED, counts, skipped, "", start);
	}

	private static PollResult result(URI feed, Outcome outcome, Counts counts, Map<SkipReason, Integer> skipped,
			String detail, long start) {
		return new PollResult(feed, outcome, counts.newEvents, counts.duplicates, skipped, detail,
				Duration.ofNanos(System.nanoTime() - start));
	}

	private static void log(PollResult result) {
		switch (result.outcome()) {
			case PUBLISHED -> log.info("rss poll {}: new={} duplicates={} skipped={} {} in {}ms", result.feed(),
					result.newEvents(), result.duplicates(), result.skippedTotal(), result.skipped(),
					result.duration().toMillis());
			case NOT_MODIFIED -> log.info("rss poll {}: not modified in {}ms", result.feed(),
					result.duration().toMillis());
			default -> log.warn("rss poll {}: {} new={} duplicates={} ({}) in {}ms", result.feed(), result.outcome(),
					result.newEvents(), result.duplicates(), result.detail(), result.duration().toMillis());
		}
	}

	/** Tallies what the sink did with each event of one poll. */
	private static final class Counts {

		int newEvents;
		int duplicates;

		void add(Accepted accepted) {
			switch (accepted) {
				case NEW -> newEvents++;
				case DUPLICATE -> duplicates++;
			}
		}

	}

	private static String rootMessage(Throwable ex) {
		Throwable root = ex;
		while (root.getCause() != null) {
			root = root.getCause();
		}
		return root.getMessage();
	}

}
