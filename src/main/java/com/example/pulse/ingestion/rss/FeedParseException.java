package com.example.pulse.ingestion.rss;

/** A feed document could not be parsed at all: malformed XML, unknown format, or a forbidden DOCTYPE. */
class FeedParseException extends RuntimeException {

	public FeedParseException(String message, Throwable cause) {
		super(message, cause);
	}

}
