package com.example.pulse.event;

/**
 * Where facts learned about events later are recorded. A port like {@link EventSink}: ingestion
 * publishes into it without knowing what stores them.
 * <p>
 * Contract: idempotent on {@code (eventId, kind)}. Recording the same fact twice has the same effect
 * as recording it once. The event itself need not exist yet: annotations and events can arrive in
 * either order.
 */
@FunctionalInterface
public interface EventAnnotations {

	/** Returns true if the fact was new, false if it was already known. */
	boolean annotate(EventAnnotation annotation);

}
