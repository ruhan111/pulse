package com.example.pulse.ingestion.rss;

import com.example.pulse.event.EventSink;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.util.unit.DataSize;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class RssConfigurationTest {

	private final ApplicationContextRunner context = new ApplicationContextRunner()
		.withUserConfiguration(RssConfiguration.class)
		.withBean(EventSink.class, () -> event -> { })
		.withBean(Clock.class, Clock::systemUTC);

	@Test
	void isOffByDefault() {
		context.run(app -> assertThat(app).doesNotHaveBean(RssPoller.class));
	}

	@Test
	void bindsPropertiesAndCreatesPollerWhenEnabled() {
		context
			.withPropertyValues(
					"pulse.rss.enabled=true",
					"pulse.rss.feeds[0]=http://127.0.0.1:9/feed.xml", // closed port: polling starts at once, keep it local
					"pulse.rss.poll-interval=1h",
					"pulse.rss.max-feed-size=2MB")
			.run(app -> {
				assertThat(app).hasSingleBean(RssPoller.class);
				RssProperties properties = app.getBean(RssProperties.class);
				assertThat(properties.feeds()).containsExactly(URI.create("http://127.0.0.1:9/feed.xml"));
				assertThat(properties.pollInterval()).isEqualTo(Duration.ofHours(1));
				assertThat(properties.fetchDeadline()).isEqualTo(Duration.ofSeconds(10));
				assertThat(properties.maxFeedSize()).isEqualTo(DataSize.ofMegabytes(2));
			});
	}

	@Test
	void failsToStartWithoutAnEventSink() {
		new ApplicationContextRunner()
			.withUserConfiguration(RssConfiguration.class)
			.withBean(Clock.class, Clock::systemUTC)
			.withPropertyValues("pulse.rss.enabled=true")
			.run(app -> assertThat(app).hasFailed());
	}

}
