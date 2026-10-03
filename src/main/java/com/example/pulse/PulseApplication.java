package com.example.pulse;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.time.Clock;

@SpringBootApplication
public class PulseApplication {

	public static void main(String[] args) {
		SpringApplication.run(PulseApplication.class, args);
	}

	/** One clock for the whole app, so tests can replace it and all timestamps agree. */
	@Bean
	Clock clock() {
		return Clock.systemUTC();
	}

}
