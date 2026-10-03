package com.example.pulse;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

// application.yaml polls real feeds; tests must not depend on the internet.
@SpringBootTest(properties = "pulse.rss.enabled=false")
class PulseApplicationTests {

	@Test
	void contextLoads() {
	}

}
