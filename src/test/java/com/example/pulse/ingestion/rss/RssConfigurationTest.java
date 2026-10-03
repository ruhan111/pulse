package com.example.pulse.ingestion.rss;

import com.example.pulse.event.EventSink;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.util.unit.DataSize;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class RssConfigurationTest {

	// Closed local port: polling starts as soon as the context does, so keep it off the internet.
	private static final String LOCAL_FEED = "http://127.0.0.1:9/feed.xml";

	private final ApplicationContextRunner bare = new ApplicationContextRunner()
		.withUserConfiguration(RssConfiguration.class)
		.withBean(EventSink.class, () -> event -> { })
		.withBean(Clock.class, Clock::systemUTC);

	/** Loads the real application.yaml, so these tests check the shipped configuration. */
	private final ApplicationContextRunner shipped = bare
		.withInitializer(new ConfigDataApplicationContextInitializer())
		.withPropertyValues("pulse.rss.feeds[0]=" + LOCAL_FEED);

	@Test
	void isOffWhenNotEnabled() {
		bare.run(app -> assertThat(app).doesNotHaveBean(RssPoller.class));
	}

	@Test
	void bindsApplicationYaml() {
		shipped.run(app -> {
			assertThat(app).hasSingleBean(RssPoller.class);
			RssProperties properties = app.getBean(RssProperties.class);
			assertThat(properties.enabled()).isTrue();
			// An overriding source replaces the whole list instead of merging into it.
			assertThat(properties.feeds()).containsExactly(URI.create(LOCAL_FEED));
			assertThat(properties.pollInterval()).isEqualTo(Duration.ofMinutes(5));
			assertThat(properties.fetchDeadline()).isEqualTo(Duration.ofSeconds(10));
			assertThat(properties.maxFeedSize()).isEqualTo(DataSize.ofMegabytes(5));
		});
	}

	@Test
	void failsToStartWhenASettingIsMissing() {
		bare
			.withPropertyValues("pulse.rss.enabled=true")
			.run(app -> assertThat(app).hasFailed()
				.getFailure().rootCause().hasMessageContaining("pulse.rss.poll-interval must be set"));
	}

	@Test
	void failsToStartWithoutAnEventSink() {
		new ApplicationContextRunner()
			.withInitializer(new ConfigDataApplicationContextInitializer())
			.withUserConfiguration(RssConfiguration.class)
			.withBean(Clock.class, Clock::systemUTC)
			.withPropertyValues("pulse.rss.feeds[0]=" + LOCAL_FEED)
			.run(app -> assertThat(app).hasFailed()
				.getFailure().hasMessageContaining(EventSink.class.getName()));
	}

}
