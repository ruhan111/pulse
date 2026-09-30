package com.example.pulse.trend;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopicTest {

	@Test
	void differentSpellingsOfSameTopicAreEqual() {
		assertThat(Topic.term("  Large   Language Models ")).isEqualTo(Topic.term("large language models"));
	}

	@Test
	void sameValueOfDifferentKindIsDifferentTopic() {
		assertThat(Topic.term("github.com")).isNotEqualTo(Topic.domain("github.com"));
	}

	@Test
	void rejectsBlankValue() {
		assertThatThrownBy(() -> Topic.term("  ")).isInstanceOf(IllegalArgumentException.class);
	}

}
