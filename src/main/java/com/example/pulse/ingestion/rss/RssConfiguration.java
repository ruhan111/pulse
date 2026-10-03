package com.example.pulse.ingestion.rss;

import com.example.pulse.event.EventSink;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;

import java.time.Clock;

/**
 * Wires RSS ingestion and schedules polling. The only entry point into {@code ingestion.rss}: every
 * other class in the package is package-private.
 * <p>
 * Requires an {@link EventSink} bean; ingestion publishes into it without knowing what it is.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "pulse.rss", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(RssProperties.class)
@EnableScheduling
public class RssConfiguration {

	private final RssProperties properties;
	private final EventSink sink;
	private final Clock clock;
	private final MeterRegistry registry;

	RssConfiguration(RssProperties properties, EventSink sink, Clock clock, MeterRegistry registry) {
		this.properties = properties;
		this.sink = sink;
		this.clock = clock;
		this.registry = registry;
	}

	@Bean
	RssPoller rssPoller() {
		FeedFetcher fetcher = new HttpFeedFetcher(properties.fetchDeadline(), (int) properties.maxFeedSize().toBytes());
		return new RssPoller(properties.feeds(), fetcher, new RssFeedParser(), new RssEventMapper(clock), sink,
				new RssMetrics(registry));
	}

	/**
	 * Fixed delay rather than fixed rate: the next round starts a fixed time after the previous one
	 * finished, so slow feeds can never make rounds overlap and pile up.
	 */
	@Bean
	SchedulingConfigurer rssPollingSchedule(RssPoller poller) {
		return registrar -> registrar.addFixedDelayTask(poller::pollAll, properties.pollInterval());
	}

}
