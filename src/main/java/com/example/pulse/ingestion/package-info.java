/**
 * Getting data in: one adapter per source (e.g. {@code ingestion.rss}) that fetches or receives
 * external data, converts it into {@link com.example.pulse.event.PulseEvent}s and publishes them to
 * an {@link com.example.pulse.event.EventSink}.
 * <p>
 * Rules: depends on {@code event}; must not depend on {@code trend}, {@code api} or
 * {@code infrastructure}. Adapters don't know what happens to events after they're published.
 */
package com.example.pulse.ingestion;
