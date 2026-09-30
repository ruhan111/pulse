package com.example.pulse.ingestion.rss;

import com.rometools.rome.feed.synd.SyndFeed;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RssFeedParserTest {

	private final RssFeedParser parser = new RssFeedParser();

	@Test
	void parsesRss2() {
		SyndFeed feed = parser.parse(resource("rss2.xml"));

		assertThat(feed.getTitle()).isEqualTo("Example News");
		assertThat(feed.getEntries()).hasSize(8);
	}

	@Test
	void parsesAtom() {
		SyndFeed feed = parser.parse(resource("atom.xml"));

		assertThat(feed.getTitle()).isEqualTo("Example Atom Blog");
		assertThat(feed.getEntries()).hasSize(1);
	}

	@Test
	void rejectsDoctypeSoExternalEntitiesCannotBeResolved() {
		assertThatThrownBy(() -> parser.parse(resource("xxe.xml")))
			.isInstanceOf(FeedParseException.class);
	}

	@Test
	void rejectsSomethingThatIsNotAFeed() {
		InputStream html = new ByteArrayInputStream("<html><body>404</body></html>".getBytes(StandardCharsets.UTF_8));

		assertThatThrownBy(() -> parser.parse(html)).isInstanceOf(FeedParseException.class);
	}

	static InputStream resource(String name) {
		return RssFeedParserTest.class.getResourceAsStream("/rss/" + name);
	}

}
