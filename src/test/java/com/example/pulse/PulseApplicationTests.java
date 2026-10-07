package com.example.pulse;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

// application.yaml polls real feeds; tests must not depend on the internet.
@SpringBootTest(properties = { "pulse.rss.enabled=false", "pulse.wikipedia.enabled=false" })
class PulseApplicationTests {

	@DynamicPropertySource
	static void database(DynamicPropertyRegistry registry) {
		TestDatabase.register(registry);
	}

	@Test
	void contextLoads() {
	}

}
