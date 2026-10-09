package com.example.pulse.ingestion.wikipedia;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class ResumePositionTest {

	private static final Duration MARGIN = Duration.ofSeconds(5);

	@Test
	void movesTimestampsBackAndKeepsTheServersFormat() {
		// A real id from the capture.
		String id = "[{\"topic\":\"eqiad.mediawiki.recentchange\",\"partition\":0,\"timestamp\":1791110065115},"
				+ "{\"topic\":\"codfw.mediawiki.recentchange\",\"partition\":0,\"offset\":-1}]";

		assertThat(ResumePosition.rewind(id, MARGIN)).isEqualTo(
				"[{\"topic\":\"eqiad.mediawiki.recentchange\",\"partition\":0,\"timestamp\":1791110060115},"
						+ "{\"topic\":\"codfw.mediawiki.recentchange\",\"partition\":0,\"offset\":-1}]");
	}

	@Test
	void rewindsEveryStreamOfACombinedConnection() {
		// A real id from the two-stream connection: one position per stream and datacenter.
		String id = "[{\"topic\":\"eqiad.mediawiki.recentchange\",\"partition\":0,\"timestamp\":1791486740974},"
				+ "{\"topic\":\"codfw.mediawiki.recentchange\",\"partition\":0,\"offset\":-1},"
				+ "{\"topic\":\"eqiad.mediawiki.revision-tags-change\",\"partition\":0,\"timestamp\":1791486740905},"
				+ "{\"topic\":\"codfw.mediawiki.revision-tags-change\",\"partition\":0,\"offset\":-1}]";

		assertThat(ResumePosition.rewind(id, MARGIN))
			.contains("\"timestamp\":1791486735974")
			.contains("\"timestamp\":1791486735905")
			.contains("\"offset\":-1");
	}

	@Test
	void leavesOffsetsAlone() {
		String id = "[{\"topic\":\"t\",\"partition\":0,\"offset\":42}]";

		assertThat(ResumePosition.rewind(id, MARGIN)).isEqualTo(id);
	}

	@Test
	void returnsWhatItDoesNotUnderstandUnchanged() {
		assertThat(ResumePosition.rewind("", MARGIN)).isEmpty();
		assertThat(ResumePosition.rewind("garbage", MARGIN)).isEqualTo("garbage");
		assertThat(ResumePosition.rewind("42", MARGIN)).isEqualTo("42");
		assertThat(ResumePosition.rewind("{\"timestamp\":5}", MARGIN)).isEqualTo("{\"timestamp\":5}");
	}

}
