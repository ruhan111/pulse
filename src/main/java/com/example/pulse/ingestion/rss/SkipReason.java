package com.example.pulse.ingestion.rss;

/** Why a feed entry could not become a {@link com.example.pulse.event.PulseEvent}. Countable, so it can become a metric. */
enum SkipReason {

	MISSING_TITLE,
	MISSING_LINK,

	/** Unparseable, or not http(s), e.g. {@code javascript:} or {@code mailto:}. */
	INVALID_LINK,

	/** Passed the checks above but still violated a {@code PulseEvent} invariant. */
	INVALID_ENTRY

}
