package com.example.pulse.ingestion.rss;

import com.example.pulse.event.EventType;
import com.example.pulse.event.PulseEvent;
import com.example.pulse.event.Source;
import com.example.pulse.ingestion.rss.MappedEntry.Mapped;
import com.example.pulse.ingestion.rss.MappedEntry.Skipped;
import com.rometools.rome.feed.synd.SyndContentImpl;
import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndEntryImpl;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static com.example.pulse.ingestion.rss.RssFeedParserTest.resource;
import static org.assertj.core.api.Assertions.assertThat;

class RssEventMapperTest {

	private static final URI FEED_URL = URI.create("https://news.example.com/feed.xml");
	private static final Instant NOW = Instant.parse("2026-09-30T12:00:00Z");

	private final RssFeedParser parser = new RssFeedParser();
	private final RssEventMapper mapper = new RssEventMapper(Clock.fixed(NOW, ZoneOffset.UTC));

	@Test
	void mapsCompleteItem() {
		PulseEvent event = mapped(rss2Entry(0));

		assertThat(event.source()).isEqualTo(Source.RSS);
		assertThat(event.channel()).isEqualTo(FEED_URL.toString());
		assertThat(event.externalId()).isEqualTo("article-1001");
		assertThat(event.type()).isEqualTo(EventType.PUBLISHED);
		assertThat(event.occurredAt()).isEqualTo(Instant.parse("2026-09-30T10:15:00Z"));
		assertThat(event.ingestedAt()).isEqualTo(NOW);
		assertThat(event.url()).isEqualTo(URI.create("https://news.example.com/articles/rust-2"));
		assertThat(event.attributes())
			.containsEntry("author", "jane@example.com (Jane Doe)")
			.containsEntry("categories", "Programming, Rust");
	}

	@Test
	void stripsHtmlFromTitleAndSummary() {
		PulseEvent event = mapped(rss2Entry(0));

		assertThat(event.title()).isEqualTo("Rust 2.0 released");
		assertThat(event.summary()).isEqualTo("The Rust team announced & shipped a new release.");
	}

	@Test
	void fallsBackToLinkWhenGuidIsMissing() {
		assertThat(mapped(rss2Entry(1)).externalId()).isEqualTo("https://news.example.com/articles/no-guid");
	}

	@Test
	void resolvesRelativeLinkAgainstFeedUrl() {
		assertThat(mapped(rss2Entry(2)).url()).isEqualTo(URI.create("https://news.example.com/articles/relative"));
	}

	@Test
	void usesIngestionTimeWhenEntryHasNoDate() {
		assertThat(mapped(rss2Entry(2)).occurredAt()).isEqualTo(NOW);
	}

	@Test
	void skipsEntryWithoutTitle() {
		assertThat(skipped(rss2Entry(3)).reason()).isEqualTo(SkipReason.MISSING_TITLE);
	}

	@Test
	void skipsLinkThatIsNotHttp() {
		assertThat(skipped(rss2Entry(4)).reason()).isEqualTo(SkipReason.INVALID_LINK);
	}

	@Test
	void skipsEntryWithoutLink() {
		assertThat(skipped(rss2Entry(5)).reason()).isEqualTo(SkipReason.MISSING_LINK);
	}

	@Test
	void doesNotTurnNonUrlGuidIntoLink() {
		assertThat(skipped(rss2Entry(6)).reason()).isEqualTo(SkipReason.MISSING_LINK);
	}

	@Test
	void usesGuidAsLinkWhenItIsARealPermalink() {
		PulseEvent event = mapped(rss2Entry(7));

		assertThat(event.url()).isEqualTo(URI.create("https://news.example.com/articles/permalink"));
		assertThat(event.externalId()).isEqualTo("https://news.example.com/articles/permalink");
	}

	@Test
	void mapsAtomEntryUsingUpdatedDateWhenNotPublished() {
		SyndEntry entry = parser.parse(resource("atom.xml")).getEntries().getFirst();

		PulseEvent event = mapped(entry);

		assertThat(event.externalId()).isEqualTo("tag:blog.example.org,2026:entry-1");
		assertThat(event.title()).isEqualTo("Atom entry one");
		assertThat(event.occurredAt()).isEqualTo(Instant.parse("2026-09-30T11:00:00Z"));
		assertThat(event.summary()).isEqualTo("Plain summary");
		assertThat(event.attributes()).containsEntry("author", "Sam");
	}

	@Test
	void sameEntryHasSameIdOnEveryPoll() {
		RssEventMapper nextPoll = new RssEventMapper(Clock.fixed(NOW.plusSeconds(600), ZoneOffset.UTC));

		PulseEvent first = mapped(rss2Entry(0));
		PulseEvent second = ((Mapped) nextPoll.map(rss2Entry(0), FEED_URL)).event();

		assertThat(second.id()).isEqualTo(first.id());
		assertThat(second.ingestedAt()).isAfter(first.ingestedAt());
	}

	@Test
	void truncatesLongSummary() {
		SyndEntry entry = new SyndEntryImpl();
		entry.setTitle("Long");
		entry.setLink("https://news.example.com/long");
		SyndContentImpl description = new SyndContentImpl();
		description.setValue("word ".repeat(1_000));
		entry.setDescription(description);

		String summary = mapped(entry).summary();

		assertThat(summary).hasSizeLessThanOrEqualTo(RssEventMapper.MAX_SUMMARY_LENGTH + 1).endsWith("…");
	}

	private SyndEntry rss2Entry(int index) {
		List<SyndEntry> entries = parser.parse(resource("rss2.xml")).getEntries();
		return entries.get(index);
	}

	private PulseEvent mapped(SyndEntry entry) {
		MappedEntry result = mapper.map(entry, FEED_URL);
		assertThat(result).isInstanceOf(Mapped.class);
		return ((Mapped) result).event();
	}

	private Skipped skipped(SyndEntry entry) {
		MappedEntry result = mapper.map(entry, FEED_URL);
		assertThat(result).isInstanceOf(Skipped.class);
		return (Skipped) result;
	}

}
