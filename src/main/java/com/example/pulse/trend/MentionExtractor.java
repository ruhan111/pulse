package com.example.pulse.trend;

import com.example.pulse.event.PulseEvent;

import java.util.List;

/**
 * Finds the topics an event is about. Implementations arrive in Phase 5, starting simple: title
 * terms without stopwords, and linked domains.
 */
@FunctionalInterface
public interface MentionExtractor {

	List<Mention> extract(PulseEvent event);

}
