package com.example.pulse.ingestion.rss;

/** Why a feed entry could not become a {@link com.example.pulse.event.PulseEvent}. Countable, so it can become a metric. */
public enum SkipReason {

	MISSING_TITLE,
	MISSING_LINK,

	/** Unparseable, or not http(s), e.g. {@code javascript:} or {@code mailto:}. */
	INVALID_LINK

}
