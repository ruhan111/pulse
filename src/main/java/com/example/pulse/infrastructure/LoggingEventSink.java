package com.example.pulse.infrastructure;

import com.example.pulse.event.EventId;
import com.example.pulse.event.EventSink;
import com.example.pulse.event.PulseEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Temporary {@link EventSink} until Phase 3 stores events in PostgreSQL: logs each new event once.
 * <p>
 * Honors the sink's idempotency contract by remembering recently seen ids, so re-polled items
 * aren't logged again. Memory is bounded: only the most recent {@value #REMEMBERED_IDS} ids are
 * kept, so a very old item could be logged twice. Acceptable for a sink whose only job is to make
 * ingestion visible; the database will give the real guarantee.
 */
@Component
class LoggingEventSink implements EventSink {

	static final int REMEMBERED_IDS = 10_000;

	private static final Logger log = LoggerFactory.getLogger(LoggingEventSink.class);

	private final Set<EventId> seen = Collections.newSetFromMap(new LinkedHashMap<>() {

		@Override
		protected boolean removeEldestEntry(Map.Entry<EventId, Boolean> eldest) {
			return size() > REMEMBERED_IDS;
		}

	});

	@Override
	public synchronized Accepted accept(PulseEvent event) {
		if (!seen.add(event.id())) {
			return Accepted.DUPLICATE;
		}
		log.info("new event {} {} | {} | {}", event.source(), event.occurredAt(), event.title(), event.url());
		return Accepted.NEW;
	}

}
