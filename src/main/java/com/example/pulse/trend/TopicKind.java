package com.example.pulse.trend;

/** What kind of thing a {@link Topic} is. Each kind is extracted and normalized differently. */
public enum TopicKind {

	/** A word or phrase from an event's text, e.g. "rust", "openai". */
	TERM,

	/** A website, e.g. "github.com". Linked domains trend when many items point to the same site. */
	DOMAIN

}
