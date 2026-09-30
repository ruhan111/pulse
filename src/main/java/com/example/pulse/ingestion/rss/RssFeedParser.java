package com.example.pulse.ingestion.rss;

import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.io.FeedException;
import com.rometools.rome.io.SyndFeedInput;
import com.rometools.rome.io.XmlReader;

import java.io.IOException;
import java.io.InputStream;

/**
 * Turns raw feed bytes into Rome's format-independent model. Handles RSS 0.9x, 1.0, 2.0 and Atom.
 * <p>
 * DOCTYPEs are rejected. Feeds never need them, and allowing them opens the door to XML external
 * entity (XXE) attacks, where a feed makes the parser read local files or call internal URLs.
 */
public class RssFeedParser {

	public SyndFeed parse(InputStream xml) {
		SyndFeedInput input = new SyndFeedInput();
		input.setAllowDoctypes(false);
		// XmlReader detects the encoding from the XML prolog/BOM instead of assuming UTF-8.
		try (XmlReader reader = new XmlReader(xml)) {
			return input.build(reader);
		}
		catch (IOException | FeedException | IllegalArgumentException ex) {
			throw new FeedParseException("Could not parse feed", ex);
		}
	}

}
