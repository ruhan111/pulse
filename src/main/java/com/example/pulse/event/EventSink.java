package com.example.pulse.event;

/**
 * Where ingested events go. Source adapters publish into this port and never know what is behind it.
 * <p>
 * Contract: implementations must be idempotent on {@link PulseEvent#id()}. Accepting the same
 * event twice has the same effect as accepting it once, and the second call reports
 * {@link Accepted#DUPLICATE}.
 */
@FunctionalInterface
public interface EventSink {

	Accepted accept(PulseEvent event);

	/**
	 * What the sink did with an event. Lets callers report how many events were really new, e.g.
	 * "new=0 duplicates=25" for a feed that resends its full contents on every poll.
	 */
	enum Accepted {

		/** First time this id was seen: the event was stored. */
		NEW,

		/** This id was seen before: nothing changed. */
		DUPLICATE

	}

}
