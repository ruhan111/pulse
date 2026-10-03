package com.example.pulse.ingestion.rss;

import com.example.pulse.event.EventId;
import com.example.pulse.event.EventSink;
import com.example.pulse.event.EventSink.Accepted;
import com.example.pulse.event.PulseEvent;
import com.example.pulse.ingestion.rss.FetchResult.Failed;
import com.example.pulse.ingestion.rss.FetchResult.Fetched;
import com.example.pulse.ingestion.rss.FetchResult.NotModified;
import com.example.pulse.ingestion.rss.PollResult.Outcome;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class RssPollerTest {

	private static final URI NEWS = URI.create("https://news.example.com/feed.xml");
	private static final URI BLOG = URI.create("https://blog.example.org/atom.xml");
	private static final CacheValidators V1 = new CacheValidators("\"v1\"", "");

	private final Map<URI, FetchResult> responses = new HashMap<>();
	private final Map<URI, List<CacheValidators>> sentValidators = new HashMap<>();
	private final List<PulseEvent> published = new ArrayList<>();
	private final Set<EventId> seen = new HashSet<>();
	private final MeterRegistry registry = new SimpleMeterRegistry();

	/** Behaves like a real sink: remembers ids and reports duplicates. */
	private final EventSink sink = event -> {
		published.add(event);
		return seen.add(event.id()) ? Accepted.NEW : Accepted.DUPLICATE;
	};

	private final FeedFetcher fetcher = (url, previous) -> {
		sentValidators.computeIfAbsent(url, key -> new ArrayList<>()).add(previous);
		FetchResult response = responses.get(url);
		if (response == null) {
			throw new IllegalStateException("bug in feed handling");
		}
		return response;
	};

	@Test
	void publishesMappedEntriesAndCountsSkippedOnes() {
		responses.put(NEWS, new Fetched(sample("rss2.xml"), V1));

		PollResult result = poller(sink).poll(NEWS);

		assertThat(result.outcome()).isEqualTo(Outcome.PUBLISHED);
		assertThat(result.newEvents()).isEqualTo(4);
		assertThat(result.duplicates()).isZero();
		assertThat(published).hasSize(4);
		assertThat(result.skipped()).containsOnly(
				Map.entry(SkipReason.MISSING_TITLE, 1),
				Map.entry(SkipReason.INVALID_LINK, 1),
				Map.entry(SkipReason.MISSING_LINK, 2));
	}

	@Test
	void reportsDuplicatesWhenFeedResendsItsContents() {
		responses.put(NEWS, new Fetched(sample("rss2.xml"), CacheValidators.NONE));
		RssPoller poller = poller(sink);

		poller.poll(NEWS);
		PollResult second = poller.poll(NEWS);

		assertThat(second.newEvents()).isZero();
		assertThat(second.duplicates()).isEqualTo(4);
	}

	@Test
	void sendsValidatorsFromPreviousFetch() {
		responses.put(NEWS, new Fetched(sample("rss2.xml"), V1));
		RssPoller poller = poller(sink);

		poller.poll(NEWS);
		responses.put(NEWS, new NotModified());
		PollResult second = poller.poll(NEWS);

		assertThat(sentValidators.get(NEWS)).containsExactly(CacheValidators.NONE, V1);
		assertThat(second.outcome()).isEqualTo(Outcome.NOT_MODIFIED);
		assertThat(published).hasSize(4);
	}

	@Test
	void pollingTwiceProducesTheSameIds() {
		responses.put(NEWS, new Fetched(sample("rss2.xml"), CacheValidators.NONE));
		RssPoller poller = poller(sink);

		poller.poll(NEWS);
		poller.poll(NEWS);

		List<EventId> first = published.subList(0, 4).stream().map(PulseEvent::id).toList();
		List<EventId> second = published.subList(4, 8).stream().map(PulseEvent::id).toList();
		assertThat(second).isEqualTo(first);
	}

	@Test
	void reportsFetchFailure() {
		responses.put(NEWS, new Failed(FetchFailure.TIMEOUT, "slow"));

		PollResult result = poller(sink).poll(NEWS);

		assertThat(result.outcome()).isEqualTo(Outcome.FETCH_FAILED);
		assertThat(result.detail()).contains("TIMEOUT");
	}

	@Test
	void reportsDocumentThatIsNotAFeed() {
		responses.put(NEWS, new Fetched("<html>maintenance</html>".getBytes(StandardCharsets.UTF_8), V1));

		PollResult result = poller(sink).poll(NEWS);

		assertThat(result.outcome()).isEqualTo(Outcome.PARSE_FAILED);
	}

	@Test
	void brokenFeedDoesNotStopTheOthers() {
		// NEWS has no response, so the fake fetcher throws: an unexpected bug, not a handled failure.
		responses.put(BLOG, new Fetched(sample("atom.xml"), V1));

		poller(sink).pollAll();

		assertThat(published).extracting(PulseEvent::channel).containsExactly(BLOG.toString());
	}

	@Test
	void keepsOldValidatorsWhenPublishingFails() {
		responses.put(NEWS, new Fetched(sample("rss2.xml"), V1));
		EventSink failsOnSecondEvent = event -> {
			if (published.size() == 1) {
				throw new IllegalStateException("sink down");
			}
			return sink.accept(event);
		};
		RssPoller poller = poller(failsOnSecondEvent);

		PollResult result = poller.poll(NEWS);
		poller.poll(NEWS);

		assertThat(result.outcome()).isEqualTo(Outcome.PUBLISH_FAILED);
		assertThat(result.newEvents()).isEqualTo(1);
		// Not V1: a 304 now would lose the events that never reached the sink.
		assertThat(sentValidators.get(NEWS)).containsExactly(CacheValidators.NONE, CacheValidators.NONE);
	}

	@Test
	void recordsEachPollWithItsOutcomeAndDuration() {
		responses.put(NEWS, new Fetched(sample("rss2.xml"), V1));
		RssPoller poller = poller(sink);

		PollResult published = poller.poll(NEWS);
		responses.put(NEWS, new NotModified());
		poller.poll(NEWS);

		Timer publishedPolls = registry.get("pulse.rss.polls").tag("feed", NEWS.toString()).tag("outcome", "PUBLISHED")
			.timer();
		assertThat(publishedPolls.count()).isEqualTo(1);
		assertThat(publishedPolls.totalTime(TimeUnit.NANOSECONDS))
			.isEqualTo(published.duration().toNanos());
		assertThat(registry.get("pulse.rss.polls").tag("outcome", "NOT_MODIFIED").timer().count()).isEqualTo(1);
	}

	@Test
	void countsSkippedEntriesByReason() {
		responses.put(NEWS, new Fetched(sample("rss2.xml"), V1));

		poller(sink).poll(NEWS);

		assertThat(skipped("MISSING_LINK")).isEqualTo(2);
		assertThat(skipped("MISSING_TITLE")).isEqualTo(1);
		assertThat(skipped("INVALID_LINK")).isEqualTo(1);
		assertThat(registry.find("pulse.rss.entries.skipped").tag("reason", "INVALID_ENTRY").counter()).isNull();
	}

	@Test
	void countsFetchFailuresByReason() {
		responses.put(NEWS, new Failed(FetchFailure.TIMEOUT, "slow"));
		RssPoller poller = poller(sink);

		poller.poll(NEWS);
		poller.poll(NEWS);

		assertThat(registry.get("pulse.rss.fetch.failures").tag("feed", NEWS.toString()).tag("reason", "TIMEOUT")
			.counter().count()).isEqualTo(2);
		assertThat(registry.get("pulse.rss.polls").tag("outcome", "FETCH_FAILED").timer().count()).isEqualTo(2);
	}

	private double skipped(String reason) {
		return registry.get("pulse.rss.entries.skipped").tag("feed", NEWS.toString()).tag("reason", reason).counter()
			.count();
	}

	private RssPoller poller(EventSink sink) {
		Clock clock = Clock.fixed(Instant.parse("2026-10-03T12:00:00Z"), ZoneOffset.UTC);
		return new RssPoller(List.of(NEWS, BLOG), fetcher, new RssFeedParser(), new RssEventMapper(clock), sink,
				new RssMetrics(registry));
	}

	private static byte[] sample(String name) {
		try (InputStream in = RssFeedParserTest.resource(name)) {
			return in.readAllBytes();
		}
		catch (IOException ex) {
			throw new UncheckedIOException(ex);
		}
	}

}
