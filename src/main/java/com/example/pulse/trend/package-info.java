/**
 * Turning events into trends: extracting {@link com.example.pulse.trend.Topic}s from events, counting
 * {@link com.example.pulse.trend.Mention}s over time, and detecting unusual activity.
 * <p>
 * Rules: depends on {@code event}; must not depend on {@code ingestion}, {@code api} or
 * {@code infrastructure}. Trend logic doesn't care where events came from or how they're stored.
 */
package com.example.pulse.trend;
