package com.example.pulse.infrastructure;

import com.example.pulse.event.CheckpointStore;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.ZoneOffset;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Stores stream positions in PostgreSQL, so a restart resumes where the app stopped instead of
 * losing everything that happened while it was down. Failures are thrown: the caller then keeps
 * reading from its last saved position, which only means re-reading a few events.
 */
@Component
class JdbcCheckpointStore implements CheckpointStore {

	static final String PARAM_STREAM = "stream";
	static final String PARAM_POSITION = "position";
	static final String PARAM_UPDATED_AT = "updatedAt";

	private final JdbcClient jdbc;
	private final Clock clock;
	private final String findCheckpoint = SqlFile.load("find_checkpoint");
	private final String upsertCheckpoint = SqlFile.load("upsert_checkpoint");
	private final String deleteCheckpoint = SqlFile.load("delete_checkpoint");

	JdbcCheckpointStore(JdbcClient jdbc, Clock clock) {
		this.jdbc = requireNonNull(jdbc, "jdbc");
		this.clock = requireNonNull(clock, "clock");
	}

	@Override
	public Optional<String> load(String stream) {
		return jdbc.sql(findCheckpoint)
			.param(PARAM_STREAM, requireNonNull(stream, "stream"))
			.query(String.class)
			.optional();
	}

	@Override
	public void save(String stream, String position) {
		requireNonNull(stream, "stream");
		requireNonNull(position, "position");
		if (position.isBlank()) {
			// A blank position would be sent back as an empty Last-Event-ID; delete() says that explicitly.
			throw new IllegalArgumentException("position must not be blank");
		}
		jdbc.sql(upsertCheckpoint)
			.param(PARAM_STREAM, stream)
			.param(PARAM_POSITION, position)
			.param(PARAM_UPDATED_AT, clock.instant().atOffset(ZoneOffset.UTC))
			.update();
	}

	@Override
	public void delete(String stream) {
		jdbc.sql(deleteCheckpoint)
			.param(PARAM_STREAM, requireNonNull(stream, "stream"))
			.update();
	}

}
