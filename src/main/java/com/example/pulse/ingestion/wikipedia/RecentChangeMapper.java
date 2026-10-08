package com.example.pulse.ingestion.wikipedia;

import com.example.pulse.event.EventType;
import com.example.pulse.event.PulseEvent;
import com.example.pulse.event.Source;
import com.example.pulse.ingestion.wikipedia.MappedChange.Mapped;
import com.example.pulse.ingestion.wikipedia.MappedChange.Skipped;
import org.jsoup.Jsoup;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.time.Clock;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static com.example.pulse.ingestion.wikipedia.JsonFields.optionalText;
import static com.example.pulse.ingestion.wikipedia.JsonFields.requiredBoolean;
import static com.example.pulse.ingestion.wikipedia.JsonFields.requiredLong;
import static com.example.pulse.ingestion.wikipedia.JsonFields.requiredText;
import static java.util.Objects.requireNonNull;

/**
 * Maps one event from Wikimedia's {@code recentchange} stream (one JSON object) to a {@link PulseEvent}.
 * Pure logic: no network, no Spring.
 * <p>
 * The stream carries every change on every Wikimedia wiki and can't be filtered server-side, so
 * most of it is skipped here. The checks run cheapest and most common first, and each one only
 * reads the fields it needs: log and categorize events have no revision, and that must not make
 * them look malformed.
 */
class RecentChangeMapper {

	static final String WIKI = "enwiki";

	private static final int ARTICLE_NAMESPACE = 0;

	private final JsonMapper json = new JsonMapper();

	private final Clock clock;

	public RecentChangeMapper(Clock clock) {
		this.clock = requireNonNull(clock, "clock");
	}

	public MappedChange map(String change) {
		try {
			return map(json.readTree(change));
		}
		catch (JacksonException ex) {
			return new Skipped(SkipReason.MALFORMED, ex.getOriginalMessage());
		}
	}

	/** For callers that have already parsed the event, e.g. to see which stream it came from. */
	public MappedChange map(JsonNode change) {
		try {
			return mapChecked(change);
		}
		catch (IllegalArgumentException | NullPointerException | DateTimeException ex) {
			// A missing field, an impossible timestamp, or a value a PulseEvent rejects. One bad event
			// must never stop the stream.
			return new Skipped(SkipReason.MALFORMED, ex.getMessage());
		}
	}

	private MappedChange mapChecked(JsonNode change) {
		String wiki = requiredText(change, "/wiki");
		if (!wiki.equals(WIKI)) {
			return new Skipped(SkipReason.OTHER_WIKI, wiki);
		}
		String type = requiredText(change, "/type");
		if (!type.equals("edit") && !type.equals("new")) {
			return new Skipped(SkipReason.NOT_AN_EDIT, type);
		}
		String title = requiredText(change, "/title");
		if (requiredLong(change, "/namespace") != ARTICLE_NAMESPACE) {
			return new Skipped(SkipReason.NOT_ARTICLE, title);
		}
		if (requiredBoolean(change, "/bot")) {
			return new Skipped(SkipReason.BOT, title);
		}

		Instant ingestedAt = clock.instant();
		return new Mapped(new PulseEvent(
				Source.WIKIPEDIA,
				wiki,
				externalId(wiki, requiredLong(change, "/revision/new")),
				type.equals("new") ? EventType.PUBLISHED : EventType.EDITED,
				Instant.ofEpochSecond(requiredLong(change, "/timestamp")),
				ingestedAt,
				title,
				url(change),
				summary(change),
				attributes(change)));
	}

	/**
	 * Revision ids are unique within a wiki and never reused, so the same edit always gets the same
	 * id. The rc {@code id} would work too, but a revision is what Wikipedia itself links to.
	 */
	static String externalId(String wiki, long revision) {
		return wiki + ":" + revision;
	}

	/** The article, not the diff: trends are about pages. */
	private static URI url(JsonNode change) {
		URI url = URI.create(requiredText(change, "/meta/uri"));
		if (!"https".equals(url.getScheme()) && !"http".equals(url.getScheme()) || url.getHost() == null) {
			throw new IllegalArgumentException("/meta/uri is not an absolute http(s) URL: " + url);
		}
		return url;
	}

	/**
	 * The edit summary, from MediaWiki's HTML rendering of it: "[[Category:X]]" becomes
	 * "Category:X". MediaWiki caps summaries at 500 characters, so there is nothing to truncate.
	 */
	private static String summary(JsonNode change) {
		String parsed = optionalText(change, "/parsedcomment");
		return parsed.isEmpty() ? optionalText(change, "/comment") : Jsoup.parseBodyFragment(parsed).text();
	}

	/** Extras only: a change without them is still a valid event. */
	private static Map<String, String> attributes(JsonNode change) {
		Map<String, String> attributes = new HashMap<>();
		String user = optionalText(change, "/user");
		if (!user.isEmpty()) {
			// Lets trend detection tell many people editing a page from one person making many edits.
			attributes.put("user", user);
		}
		JsonNode minor = change.at("/minor");
		if (minor.isBoolean()) {
			attributes.put("minor", String.valueOf(minor.booleanValue()));
		}
		JsonNode newLength = change.at("/length/new");
		if (newLength.isIntegralNumber()) {
			// A new page has no old length.
			JsonNode oldLength = change.at("/length/old");
			long delta = newLength.longValue() - (oldLength.isIntegralNumber() ? oldLength.longValue() : 0);
			attributes.put("size_delta", String.valueOf(delta));
		}
		return attributes;
	}

}
