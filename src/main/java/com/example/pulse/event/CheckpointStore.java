package com.example.pulse.event;

import java.util.Optional;

/**
 * Where a streaming source stopped reading, so it can resume there after a reconnect or a restart.
 * A port like {@link EventSink}: ingestion uses it without knowing what stores it.
 * <p>
 * Positions are opaque: whatever the source uses to resume (e.g. an SSE {@code Last-Event-ID}),
 * stored and returned unchanged. Only the latest position per stream is kept.
 */
public interface CheckpointStore {

	/** The last saved position of a stream, or empty if it has none. */
	Optional<String> load(String stream);

	/** Replaces the stream's position. Save only once everything before it has been published. */
	void save(String stream, String position);

	/** Forgets the stream's position, e.g. when the source rejects it. */
	void delete(String stream);

}
