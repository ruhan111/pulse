package com.example.pulse.infrastructure;

import com.example.pulse.TestDatabase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Against a real PostgreSQL, like the event sink: the upsert lives in SQL. */
@SpringBootTest(properties = { "pulse.rss.enabled=false", "pulse.wikipedia.enabled=false" })
class JdbcCheckpointStoreTest {

	private final String findUpdatedAt = SqlFile.load("find_checkpoint_updated_at");

	@DynamicPropertySource
	static void database(DynamicPropertyRegistry registry) {
		TestDatabase.register(registry);
	}

	@Autowired
	private JdbcCheckpointStore checkpoints;

	@Autowired
	private JdbcClient jdbc;

	@Test
	void hasNoPositionForANewStream() {
		assertThat(checkpoints.load(uniqueStream())).isEmpty();
	}

	@Test
	void returnsTheSavedPositionUnchanged() {
		String stream = uniqueStream();
		String position = "[{\"topic\":\"eqiad.mediawiki.recentchange\",\"partition\":0,\"timestamp\":1791110065115}]";

		checkpoints.save(stream, position);

		assertThat(checkpoints.load(stream)).contains(position);
	}

	@Test
	void theLatestPositionWins() {
		String stream = uniqueStream();

		checkpoints.save(stream, "1");
		checkpoints.save(stream, "2");

		assertThat(checkpoints.load(stream)).contains("2");
	}

	@Test
	void recordsWhenThePositionWasSaved() {
		String stream = uniqueStream();
		Instant before = Instant.now();

		checkpoints.save(stream, "1");

		Instant updatedAt = jdbc.sql(findUpdatedAt).param(JdbcCheckpointStore.PARAM_STREAM, stream).query(OffsetDateTime.class).single()
			.toInstant();
		assertThat(updatedAt).isBetween(before.minus(Duration.ofSeconds(1)), Instant.now().plus(Duration.ofSeconds(1)));
	}

	@Test
	void keepsStreamsApart() {
		String a = uniqueStream();
		String b = uniqueStream();

		checkpoints.save(a, "1");
		checkpoints.save(b, "2");

		assertThat(checkpoints.load(a)).contains("1");
		assertThat(checkpoints.load(b)).contains("2");
	}

	@Test
	void deleteForgetsThePosition() {
		String stream = uniqueStream();
		checkpoints.save(stream, "1");

		checkpoints.delete(stream);
		checkpoints.delete(stream);

		assertThat(checkpoints.load(stream)).isEmpty();
	}

	@Test
	void rejectsABlankPosition() {
		assertThatThrownBy(() -> checkpoints.save(uniqueStream(), " ")).isInstanceOf(IllegalArgumentException.class);
	}

	private static String uniqueStream() {
		return "test." + UUID.randomUUID();
	}

}
