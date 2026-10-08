package com.example.pulse.infrastructure;

import com.example.pulse.event.EventAnnotation;
import com.example.pulse.event.EventAnnotations;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import java.time.ZoneOffset;
import java.util.EnumMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Stores annotations in PostgreSQL. Idempotent like the event sink: the primary key is
 * {@code (event_id, kind)} and one {@code insert ... on conflict do nothing} both stores a new fact
 * and detects a known one. Failures are thrown, so the stream consumer resumes from its checkpoint
 * and the fact arrives again.
 */
@Component
class JdbcEventAnnotations implements EventAnnotations {

	static final String PARAM_EVENT_ID = "eventId";
	static final String PARAM_KIND = "kind";
	static final String PARAM_ANNOTATED_AT = "annotatedAt";

	private final JdbcClient jdbc;
	private final String insertAnnotationIfAbsent = SqlFile.load("insert_annotation_if_absent");
	private final Map<EventAnnotation.Kind, Counter> added = new EnumMap<>(EventAnnotation.Kind.class);
	private final Map<EventAnnotation.Kind, Counter> known = new EnumMap<>(EventAnnotation.Kind.class);

	JdbcEventAnnotations(JdbcClient jdbc, MeterRegistry registry) {
		this.jdbc = requireNonNull(jdbc, "jdbc");
		requireNonNull(registry, "registry");
		// Registered at 0, so a kind that hasn't occurred yet reads 0 instead of 404 (008).
		for (EventAnnotation.Kind kind : EventAnnotation.Kind.values()) {
			added.put(kind, counter(registry, kind, "NEW"));
			known.put(kind, counter(registry, kind, "DUPLICATE"));
		}
	}

	@Override
	public boolean annotate(EventAnnotation annotation) {
		boolean isNew = jdbc.sql(insertAnnotationIfAbsent)
			.param(PARAM_EVENT_ID, annotation.eventId().value())
			.param(PARAM_KIND, annotation.kind().name())
			.param(PARAM_ANNOTATED_AT, annotation.annotatedAt().atOffset(ZoneOffset.UTC))
			.update() == 1;
		(isNew ? added : known).get(annotation.kind()).increment();
		return isNew;
	}

	private static Counter counter(MeterRegistry registry, EventAnnotation.Kind kind, String result) {
		return Counter.builder("pulse.annotations.accepted")
			.description("Annotations handed to the store, by kind and whether they were new")
			.tag("kind", kind.name())
			.tag("result", result)
			.register(registry);
	}

}
