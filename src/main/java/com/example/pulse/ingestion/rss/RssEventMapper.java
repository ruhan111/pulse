package com.example.pulse.ingestion.rss;

import com.example.pulse.event.EventType;
import com.example.pulse.event.PulseEvent;
import com.example.pulse.event.Source;
import com.example.pulse.ingestion.rss.MappedEntry.Mapped;
import com.example.pulse.ingestion.rss.MappedEntry.Skipped;
import com.rometools.rome.feed.synd.SyndCategory;
import com.rometools.rome.feed.synd.SyndContent;
import com.rometools.rome.feed.synd.SyndEntry;
import org.jsoup.Jsoup;

import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Maps one parsed feed entry to a {@link PulseEvent}. Pure logic: no network, no Spring.
 * <p>
 * Every rule here exists because real feeds are messy: missing guids, missing dates, HTML in
 * titles, relative links. Entries that can't produce a valid event are skipped with a reason.
 */
class RssEventMapper {

	static final int MAX_SUMMARY_LENGTH = 1_000;

	private final Clock clock;

	public RssEventMapper(Clock clock) {
		this.clock = requireNonNull(clock, "clock");
	}

	public MappedEntry map(SyndEntry entry, URI feedUrl) {
		String title = toText(entry.getTitle());
		if (title.isEmpty()) {
			return new Skipped(SkipReason.MISSING_TITLE, entry.getLink());
		}
		if (isBlank(entry.getLink()) || isGuidCopiedAsLink(entry)) {
			return new Skipped(SkipReason.MISSING_LINK, title);
		}
		Optional<URI> url = resolveHttpUrl(feedUrl, entry.getLink());
		if (url.isEmpty()) {
			return new Skipped(SkipReason.INVALID_LINK, entry.getLink());
		}

		Instant ingestedAt = clock.instant();
		try {
			return new Mapped(toEvent(entry, feedUrl, title, url.get(), ingestedAt));
		}
		catch (IllegalArgumentException | NullPointerException ex) {
			// A backstop: one malformed entry must never break the rest of the feed.
			return new Skipped(SkipReason.INVALID_ENTRY, ex.getMessage());
		}
	}

	private static PulseEvent toEvent(SyndEntry entry, URI feedUrl, String title, URI url, Instant ingestedAt) {
		return new PulseEvent(
				Source.RSS,
				feedUrl.toString(),
				externalId(entry, url),
				EventType.PUBLISHED,
				occurredAt(entry, ingestedAt),
				ingestedAt,
				title,
				url,
				summary(entry),
				attributes(entry));
	}

	/**
	 * Must be stable across polls, because {@code EventId} is derived from it. Rome already puts the
	 * guid (RSS) or id (Atom) in {@code uri} and falls back to the link when there is none; the
	 * resolved url is a last resort for entries where Rome leaves it empty.
	 */
	private static String externalId(SyndEntry entry, URI url) {
		return isBlank(entry.getUri()) ? url.toString() : entry.getUri();
	}

	/**
	 * When an RSS item has no link but a guid with the default {@code isPermaLink="true"}, Rome copies
	 * the guid into the link. Feeds often do this with guids that aren't URLs at all ("article-1006"),
	 * and resolving that against the feed URL would invent a page that doesn't exist. A guid only
	 * counts as a link if it is already absolute.
	 */
	private static boolean isGuidCopiedAsLink(SyndEntry entry) {
		String link = entry.getLink().strip();
		return link.equals(entry.getUri()) && !link.contains("://");
	}

	/** Published, else updated, else the time we saw it. Some feeds carry no dates at all. */
	private static Instant occurredAt(SyndEntry entry, Instant ingestedAt) {
		Date date = entry.getPublishedDate() != null ? entry.getPublishedDate() : entry.getUpdatedDate();
		return date != null ? date.toInstant() : ingestedAt;
	}

	/** Relative links are resolved against the feed; anything that isn't http(s) is rejected. */
	private static Optional<URI> resolveHttpUrl(URI feedUrl, String link) {
		try {
			URI url = feedUrl.resolve(link.strip());
			String scheme = url.getScheme();
			boolean http = "http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme);
			return http && url.getHost() != null ? Optional.of(url) : Optional.empty();
		}
		catch (IllegalArgumentException ex) {
			return Optional.empty();
		}
	}

	private static String summary(SyndEntry entry) {
		SyndContent content = entry.getDescription();
		if (content == null && !entry.getContents().isEmpty()) {
			content = entry.getContents().getFirst();
		}
		return content == null ? "" : truncate(toText(content.getValue()), MAX_SUMMARY_LENGTH);
	}

	private static Map<String, String> attributes(SyndEntry entry) {
		Map<String, String> attributes = new HashMap<>();
		if (!isBlank(entry.getAuthor())) {
			attributes.put("author", entry.getAuthor().strip());
		}
		String categories = entry.getCategories().stream()
			.map(SyndCategory::getName)
			.filter(name -> !isBlank(name))
			.map(String::strip)
			.collect(Collectors.joining(", "));
		if (!categories.isEmpty()) {
			attributes.put("categories", categories);
		}
		return attributes;
	}

	/** HTML to plain text: removes tags and script content, decodes entities, collapses whitespace. */
	private static String toText(String html) {
		return html == null ? "" : Jsoup.parseBodyFragment(html).text().strip();
	}

	private static String truncate(String text, int maxLength) {
		if (text.length() <= maxLength) {
			return text;
		}
		int end = Character.isHighSurrogate(text.charAt(maxLength - 1)) ? maxLength - 1 : maxLength;
		return text.substring(0, end).strip() + "…";
	}

	private static boolean isBlank(String value) {
		return value == null || value.isBlank();
	}

}
