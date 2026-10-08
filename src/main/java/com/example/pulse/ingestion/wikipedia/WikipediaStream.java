package com.example.pulse.ingestion.wikipedia;

import tools.jackson.databind.JsonNode;

/**
 * The Wikimedia streams Pulse reads, all over one connection (journal 013): one thread, one
 * checkpoint, and both streams always resume from the same position.
 */
enum WikipediaStream {

	/** Every edit on every wiki; kept edits become events. */
	RECENT_CHANGES("mediawiki.recentchange"),

	/** Tags added to revisions, e.g. "reverted"; relevant ones become annotations. */
	REVISION_TAGS("mediawiki.revision-tags-change");

	private final String streamName;

	WikipediaStream(String streamName) {
		this.streamName = streamName;
	}

	/**
	 * Which stream an event came from, by its {@code meta.stream}. Anything that isn't recognisably a
	 * tag change goes to the recent-change mapper, which rejects it as malformed if it isn't one.
	 */
	static WikipediaStream of(JsonNode event) {
		return REVISION_TAGS.streamName.equals(JsonFields.optionalText(event, "/meta/stream"))
				? REVISION_TAGS
				: RECENT_CHANGES;
	}

}
