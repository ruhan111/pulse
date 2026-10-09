package com.example.pulse.ingestion.wikipedia;

import com.example.pulse.event.EventAnnotation;
import com.example.pulse.event.EventAnnotation.Kind;
import com.example.pulse.event.EventId;
import com.example.pulse.event.Source;
import com.example.pulse.ingestion.wikipedia.MappedChange.Annotated;
import com.example.pulse.ingestion.wikipedia.MappedChange.Skipped;
import tools.jackson.databind.JsonNode;

import java.time.DateTimeException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.example.pulse.ingestion.wikipedia.JsonFields.requiredLong;
import static com.example.pulse.ingestion.wikipedia.JsonFields.requiredText;

/**
 * Maps one event from Wikimedia's {@code mediawiki.revision-tags-change} stream to annotations of
 * the edit it is about. Pure logic: no network, no Spring.
 * <p>
 * MediaWiki tags reverts itself, by comparing content, which catches reverts that edit summaries
 * don't announce (journal 013: 12 reverts by tags against 3 by summary in the same 3 minutes). The
 * revert edit carries {@code mw-undo}, {@code mw-rollback} or {@code mw-manual-revert} when it is
 * saved; the edit it undid gets {@code mw-reverted} when the revert happens.
 * <p>
 * The annotation points at the edit through the same {@link EventId} as {@link RecentChangeMapper}
 * gives it, derived from the revision id, so the two streams join without either knowing the other.
 */
class TagChangeMapper {

	private static final Map<String, Kind> TAGS = Map.of(
			"mw-undo", Kind.REVERT,
			"mw-rollback", Kind.REVERT,
			"mw-manual-revert", Kind.REVERT,
			"mw-reverted", Kind.REVERTED,
			"mw-new-redirect", Kind.REDIRECT);

	private static final int ARTICLE_NAMESPACE = 0;

	MappedChange map(JsonNode change) {
		try {
			return mapChecked(change);
		}
		catch (IllegalArgumentException | NullPointerException | DateTimeException ex) {
			// A missing field or an unparseable time. One bad event must never stop the stream.
			return new Skipped(SkipReason.MALFORMED, ex.getMessage());
		}
	}

	private MappedChange mapChecked(JsonNode change) {
		String wiki = requiredText(change, "/database");
		if (!wiki.equals(RecentChangeMapper.WIKI)) {
			return new Skipped(SkipReason.OTHER_WIKI, wiki);
		}
		if (requiredLong(change, "/page_namespace") != ARTICLE_NAMESPACE) {
			// Pulse never stores those edits, so facts about them would point at nothing.
			return new Skipped(SkipReason.NOT_ARTICLE, requiredText(change, "/page_title"));
		}
		Set<Kind> kinds = new HashSet<>();
		for (String tag : addedTags(change)) {
			Kind kind = TAGS.get(tag);
			if (kind != null) {
				kinds.add(kind);
			}
		}
		if (kinds.isEmpty()) {
			return new Skipped(SkipReason.NO_RELEVANT_TAG, "");
		}

		EventId edit = EventId.of(Source.WIKIPEDIA,
				RecentChangeMapper.externalId(wiki, requiredLong(change, "/rev_id")));
		Instant annotatedAt = Instant.parse(requiredText(change, "/meta/dt"));
		List<EventAnnotation> annotations = new ArrayList<>();
		for (Kind kind : Kind.values()) {
			if (kinds.contains(kind)) {
				annotations.add(new EventAnnotation(edit, kind, annotatedAt));
			}
		}
		return new Annotated(annotations);
	}

	/**
	 * Only the tags this change added. The event repeats all of the revision's tags, including ones
	 * reported before ({@code prior_state}), which would otherwise be recorded again.
	 */
	private static Set<String> addedTags(JsonNode change) {
		Set<String> added = strings(change.at("/tags"));
		added.removeAll(strings(change.at("/prior_state/tags")));
		return added;
	}

	private static Set<String> strings(JsonNode array) {
		Set<String> strings = new HashSet<>();
		for (JsonNode element : array) {
			if (element.isString()) {
				strings.add(element.stringValue());
			}
		}
		return strings;
	}

}
