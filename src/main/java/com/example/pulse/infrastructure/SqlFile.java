package com.example.pulse.infrastructure;

import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;

/**
 * Loads SQL from {@code src/main/resources/sql/}, one statement per file, named after what it does.
 * Queries stay readable as SQL and reviewable on their own instead of hiding in Java strings.
 */
final class SqlFile {

	private SqlFile() {
	}

	/**
	 * Read at construction time by the classes that use it, so a missing or misnamed file stops the
	 * app at startup instead of failing on the first query.
	 */
	static String load(String name) {
		requireNonNull(name, "name");
		ClassPathResource resource = new ClassPathResource("sql/" + name + ".sql");
		try {
			return resource.getContentAsString(StandardCharsets.UTF_8).strip();
		}
		catch (IOException ex) {
			throw new UncheckedIOException("cannot read " + resource.getPath(), ex);
		}
	}

}
