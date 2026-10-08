package com.example.pulse.ingestion.wikipedia;

import com.example.pulse.ingestion.wikipedia.StreamTally.Snapshot;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.example.pulse.ingestion.wikipedia.WikipediaStream.RECENT_CHANGES;
import static com.example.pulse.ingestion.wikipedia.WikipediaStream.REVISION_TAGS;
import static org.assertj.core.api.Assertions.assertThat;

class StreamTallyTest {

	private final StreamTally tally = new StreamTally();

	@Test
	void receivedIsEverythingHandledPerStream() {
		tally.used(RECENT_CHANGES, 1, 0);
		tally.used(RECENT_CHANGES, 0, 1);
		tally.skipped(RECENT_CHANGES, SkipReason.OTHER_WIKI);
		tally.skipped(RECENT_CHANGES, SkipReason.OTHER_WIKI);
		tally.skipped(REVISION_TAGS, SkipReason.NO_RELEVANT_TAG);

		assertThat(tally.received(RECENT_CHANGES)).isEqualTo(4);
		assertThat(tally.received(REVISION_TAGS)).isEqualTo(1);
		assertThat(tally.skippedCount(RECENT_CHANGES, SkipReason.OTHER_WIKI)).isEqualTo(2);
		assertThat(tally.skippedCount(REVISION_TAGS, SkipReason.OTHER_WIKI)).isZero();
	}

	@Test
	void anEventCanHaveSeveralOutputs() {
		// One tag change can annotate an edit as both a revert and a redirect.
		tally.used(REVISION_TAGS, 1, 1);

		Snapshot tags = tally.snapshot(REVISION_TAGS);
		assertThat(tags.received()).isEqualTo(1);
		assertThat(tags.added()).isEqualTo(1);
		assertThat(tags.duplicates()).isEqualTo(1);
	}

	@Test
	void theDifferenceOfTwoSnapshotsIsWhatHappenedInBetween() {
		tally.used(RECENT_CHANGES, 1, 0);
		tally.skipped(RECENT_CHANGES, SkipReason.OTHER_WIKI);
		Snapshot before = tally.snapshot(RECENT_CHANGES);

		tally.used(RECENT_CHANGES, 1, 0);
		tally.used(RECENT_CHANGES, 0, 1);
		tally.skipped(RECENT_CHANGES, SkipReason.OTHER_WIKI);
		tally.skipped(RECENT_CHANGES, SkipReason.NOT_ARTICLE);
		Snapshot between = tally.snapshot(RECENT_CHANGES).minus(before);

		assertThat(between.added()).isEqualTo(1);
		assertThat(between.duplicates()).isEqualTo(1);
		assertThat(between.skippedTotal()).isEqualTo(2);
		assertThat(between.received()).isEqualTo(4);
		// Only the reasons that occurred, in enum order, for the log line.
		assertThat(between.skippedReasons()).containsExactly(
				Map.entry(SkipReason.OTHER_WIKI, 1L),
				Map.entry(SkipReason.NOT_ARTICLE, 1L));
	}

}
