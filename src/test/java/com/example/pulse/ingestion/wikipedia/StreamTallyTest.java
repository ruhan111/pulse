package com.example.pulse.ingestion.wikipedia;

import com.example.pulse.event.EventSink.Accepted;
import com.example.pulse.ingestion.wikipedia.StreamTally.Snapshot;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class StreamTallyTest {

	private final StreamTally tally = new StreamTally();

	@Test
	void receivedIsEverythingHandled() {
		tally.accepted(Accepted.NEW);
		tally.accepted(Accepted.DUPLICATE);
		tally.skipped(SkipReason.OTHER_WIKI);
		tally.skipped(SkipReason.OTHER_WIKI);
		tally.skipped(SkipReason.BOT);

		assertThat(tally.received()).isEqualTo(5);
		assertThat(tally.skippedCount(SkipReason.OTHER_WIKI)).isEqualTo(2);
		assertThat(tally.skippedCount(SkipReason.MALFORMED)).isZero();
	}

	@Test
	void theDifferenceOfTwoSnapshotsIsWhatHappenedInBetween() {
		tally.accepted(Accepted.NEW);
		tally.skipped(SkipReason.OTHER_WIKI);
		Snapshot before = tally.snapshot();

		tally.accepted(Accepted.NEW);
		tally.accepted(Accepted.DUPLICATE);
		tally.skipped(SkipReason.OTHER_WIKI);
		tally.skipped(SkipReason.NOT_ARTICLE);
		Snapshot between = tally.snapshot().minus(before);

		assertThat(between.newEvents()).isEqualTo(1);
		assertThat(between.duplicates()).isEqualTo(1);
		assertThat(between.skippedTotal()).isEqualTo(2);
		assertThat(between.received()).isEqualTo(4);
		// Only the reasons that occurred, in enum order, for the log line.
		assertThat(between.skippedReasons()).containsExactly(
				Map.entry(SkipReason.OTHER_WIKI, 1L),
				Map.entry(SkipReason.NOT_ARTICLE, 1L));
	}

}
