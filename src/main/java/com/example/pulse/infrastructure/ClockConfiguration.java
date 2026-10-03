package com.example.pulse.infrastructure;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/** One clock for the whole app, so tests can replace it and all timestamps agree. */
@Configuration(proxyBeanMethods = false)
public class ClockConfiguration {

	@Bean
	Clock clock() {
		return Clock.systemUTC();
	}

}
