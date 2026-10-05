package com.example.pulse.ingestion.wikipedia;

import static java.util.Objects.requireNonNull;

/**
 * Why a stream connection ended. A stream always ends eventually, so this is a result, not an
 * exception; the reason is an enum so disconnects can be counted.
 */
record Disconnect(Reason reason, String detail) {

	Disconnect {
		requireNonNull(reason, "reason");
		detail = detail == null ? "" : detail;
	}

	enum Reason {

		/** The server closed the stream normally. */
		ENDED,

		/** Connecting failed, the connection broke, or the reading thread was interrupted. */
		NETWORK_ERROR,

		/** Any status but 200, except the case below. */
		HTTP_ERROR,

		/** 400 to a request that carried a {@code Last-Event-ID}: the server can't resume from it. */
		REJECTED_POSITION,

		/** Nothing arrived for too long; the connection was closed rather than trusted. */
		IDLE_TIMEOUT,

		/** Handling an event failed, e.g. the database is down. The event was not handled. */
		HANDLER_FAILED

	}

}
