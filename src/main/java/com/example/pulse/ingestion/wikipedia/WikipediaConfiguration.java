package com.example.pulse.ingestion.wikipedia;

import com.example.pulse.event.CheckpointStore;
import com.example.pulse.event.EventSink;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.time.Duration;

/**
 * Wires Wikipedia ingestion and runs the stream for as long as the application runs. The only entry
 * point into {@code ingestion.wikipedia}: every other class in the package is package-private.
 * <p>
 * Requires an {@link EventSink} and a {@link CheckpointStore} bean; ingestion uses both without
 * knowing what they are.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "pulse.wikipedia", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(WikipediaProperties.class)
public class WikipediaConfiguration {

	/** Not settings: nothing so far needed other values, and the journal explains these (010). */
	static final Duration INITIAL_BACKOFF = Duration.ofSeconds(1);
	static final Duration MAX_BACKOFF = Duration.ofSeconds(60);

	@Bean
	RecentChangeConsumer recentChangeConsumer(WikipediaProperties properties, EventSink sink,
			CheckpointStore checkpoints, Clock clock, MeterRegistry registry) {
		EventStreamClient client = new EventStreamClient(properties.streamUrl(), properties.connectTimeout(),
				properties.idleTimeout());
		return new RecentChangeConsumer(client, new RecentChangeMapper(clock), sink, checkpoints,
				new WikipediaMetrics(registry), clock, properties.checkpointInterval(), INITIAL_BACKOFF,
				MAX_BACKOFF);
	}

	/**
	 * Starts the consumer once the context is fully up (migrations applied) and stops it first on
	 * shutdown. Lifecycle beans stop before singletons are destroyed, so the final checkpoint is
	 * saved while the connection pool is still open.
	 */
	@Bean
	SmartLifecycle recentChangeStream(RecentChangeConsumer consumer) {
		return new SmartLifecycle() {

			@Override
			public void start() {
				consumer.start();
			}

			@Override
			public void stop() {
				try {
					consumer.stop();
				}
				catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
				}
			}

			@Override
			public boolean isRunning() {
				return consumer.isRunning();
			}

		};
	}

}
