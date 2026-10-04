package com.example.pulse.ingestion.wikipedia;

/**
 * Why a recent change did not become a {@link com.example.pulse.event.PulseEvent}. Countable, so it
 * can become a metric. Listed in the order the checks run.
 */
enum SkipReason {

	/** Not valid JSON, or a field the checks or the event need is missing or has the wrong type. */
	MALFORMED,

	/** The stream carries every Wikimedia wiki and can't be filtered server-side; only enwiki is kept. */
	OTHER_WIKI,

	/** A log entry, a category membership change or an external change rather than an edit or a new page. */
	NOT_AN_EDIT,

	/** Talk, user, category, draft and other non-article pages. */
	NOT_ARTICLE,

	/** Flagged as a bot edit. Bots make bulk edits that say nothing about what people care about. */
	BOT

}
