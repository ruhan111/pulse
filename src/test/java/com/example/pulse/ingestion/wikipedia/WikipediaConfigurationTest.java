package com.example.pulse.ingestion.wikipedia;

import com.example.pulse.event.CheckpointStore;
import com.example.pulse.event.EventAnnotations;
import com.example.pulse.event.EventSink;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.SmartLifecycle;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class WikipediaConfigurationTest {

	// Closed local port: the stream starts with the context, so keep it off the internet.
	private static final String LOCAL_STREAM = "http://127.0.0.1:9/stream";

	private final ApplicationContextRunner bare = new ApplicationContextRunner()
		.withUserConfiguration(WikipediaConfiguration.class)
		.withBean(EventSink.class, () -> event -> EventSink.Accepted.NEW)
		.withBean(CheckpointStore.class, NoCheckpoints::new)
		.withBean(EventAnnotations.class, () -> annotation -> true)
		.withBean(Clock.class, Clock::systemUTC)
		.withBean(MeterRegistry.class, SimpleMeterRegistry::new);

	/** Loads the real application.yaml, so these tests check the shipped configuration. */
	private final ApplicationContextRunner shipped = bare
		.withInitializer(new ConfigDataApplicationContextInitializer())
		.withPropertyValues("pulse.wikipedia.stream-url=" + LOCAL_STREAM);

	@Test
	void isOffWhenNotEnabled() {
		bare.run(app -> assertThat(app).doesNotHaveBean(WikipediaStreamConsumer.class));
	}

	@Test
	void bindsApplicationYaml() {
		shipped.run(app -> {
			WikipediaProperties properties = app.getBean(WikipediaProperties.class);
			assertThat(properties.enabled()).isTrue();
			assertThat(properties.streamUrl()).isEqualTo(URI.create(LOCAL_STREAM));
			assertThat(properties.connectTimeout()).isEqualTo(Duration.ofSeconds(10));
			assertThat(properties.idleTimeout()).isEqualTo(Duration.ofSeconds(30));
			assertThat(properties.checkpointInterval()).isEqualTo(Duration.ofSeconds(5));
		});
	}

	@Test
	void shipsTheRealStreamUrl() {
		bare.withInitializer(new ConfigDataApplicationContextInitializer())
			.withPropertyValues("pulse.wikipedia.enabled=false")
			.run(app -> assertThat(app.getEnvironment().getProperty("pulse.wikipedia.stream-url"))
				.isEqualTo("https://stream.wikimedia.org/v2/stream/recentchange,mediawiki.revision-tags-change"));
	}

	@Test
	void startsTheStreamWithTheContextAndStopsItOnClose() {
		WikipediaStreamConsumer[] consumer = new WikipediaStreamConsumer[1];
		shipped.run(app -> {
			consumer[0] = app.getBean(WikipediaStreamConsumer.class);
			assertThat(consumer[0].isRunning()).isTrue();
			assertThat(app.getBean("wikipediaStream", SmartLifecycle.class).isRunning()).isTrue();
		});
		assertThat(consumer[0].isRunning()).isFalse();
	}

	@Test
	void failsToStartWhenASettingIsMissing() {
		bare.withPropertyValues("pulse.wikipedia.enabled=true", "pulse.wikipedia.stream-url=" + LOCAL_STREAM)
			.run(app -> assertThat(app).hasFailed()
				.getFailure().rootCause().hasMessageContaining("pulse.wikipedia.connect-timeout must be set"));
	}

	@Test
	void failsToStartWithoutACheckpointStore() {
		new ApplicationContextRunner()
			.withInitializer(new ConfigDataApplicationContextInitializer())
			.withUserConfiguration(WikipediaConfiguration.class)
			.withBean(EventSink.class, () -> event -> EventSink.Accepted.NEW)
			.withBean(EventAnnotations.class, () -> annotation -> true)
			.withBean(Clock.class, Clock::systemUTC)
			.withBean(MeterRegistry.class, SimpleMeterRegistry::new)
			.withPropertyValues("pulse.wikipedia.stream-url=" + LOCAL_STREAM)
			.run(app -> assertThat(app).hasFailed()
				.getFailure().hasMessageContaining(CheckpointStore.class.getName()));
	}

	private static final class NoCheckpoints implements CheckpointStore {

		@Override
		public Optional<String> load(String stream) {
			return Optional.empty();
		}

		@Override
		public void save(String stream, String position) {
		}

		@Override
		public void delete(String stream) {
		}

	}

}
