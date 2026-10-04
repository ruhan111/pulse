package com.example.pulse.ingestion.wikipedia;

import com.example.pulse.event.EventId;
import com.example.pulse.event.EventType;
import com.example.pulse.event.PulseEvent;
import com.example.pulse.event.Source;
import com.example.pulse.ingestion.wikipedia.MappedChange.Mapped;
import com.example.pulse.ingestion.wikipedia.MappedChange.Skipped;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every fixture is a real event from 30 seconds of the live stream (2026-10-04, 10:34 UTC), see
 * {@code recentchange-sample.jsonl}. Malformed cases are real events with a field removed or changed.
 */
class RecentChangeMapperTest {

	private static final Instant NOW = Instant.parse("2026-10-04T10:35:00Z");

	private final RecentChangeMapper mapper = new RecentChangeMapper(Clock.fixed(NOW, ZoneOffset.UTC));

	@Test
	void mapsArticleEdit() {
		PulseEvent event = mapped(resource("enwiki-edit.json"));

		assertThat(event.source()).isEqualTo(Source.WIKIPEDIA);
		assertThat(event.channel()).isEqualTo("enwiki");
		assertThat(event.externalId()).isEqualTo("enwiki:1378416775");
		assertThat(event.type()).isEqualTo(EventType.EDITED);
		assertThat(event.occurredAt()).isEqualTo(Instant.ofEpochSecond(1791110083));
		assertThat(event.ingestedAt()).isEqualTo(NOW);
		assertThat(event.title()).isEqualTo("Tirhut division");
		assertThat(event.url()).isEqualTo(URI.create("https://en.wikipedia.org/wiki/Tirhut_division"));
		assertThat(event.attributes()).containsOnly(
				Map.entry("user", "Rodw"),
				Map.entry("minor", "true"),
				Map.entry("size_delta", "25"));
	}

	@Test
	void summaryIsEditCommentAsPlainText() {
		assertThat(mapped(resource("enwiki-edit.json")).summary()).isEqualTo(
				"Disambiguating links to Ramnagar (link changed to Ramnagar, West Champaran) using DisamAssist.");
	}

	@Test
	void summaryFallsBackToRawCommentWithoutParsedComment() {
		String change = modified("enwiki-edit.json", json -> json.remove("parsedcomment"));

		assertThat(mapped(change).summary()).startsWith("Disambiguating links to [[Ramnagar]]");
	}

	@Test
	void mapsNewPageAsPublished() {
		PulseEvent event = mapped(resource("enwiki-new.json"));

		assertThat(event.type()).isEqualTo(EventType.PUBLISHED);
		assertThat(event.externalId()).isEqualTo("enwiki:1378416740");
		// No old length: the whole page is new.
		assertThat(event.attributes()).containsEntry("size_delta", "35");
	}

	@Test
	void sameChangeAlwaysGetsSameId() {
		String change = resource("enwiki-edit.json");

		EventId first = mapped(change).id();
		EventId second = new RecentChangeMapper(Clock.systemUTC()).map(change) instanceof Mapped(PulseEvent event)
				? event.id() : null;

		assertThat(second).isEqualTo(first);
	}

	@Test
	void keepsEventWithoutOptionalFields() {
		String change = modified("enwiki-edit.json", json -> json.remove(List.of("user", "minor", "length",
				"comment", "parsedcomment")));

		PulseEvent event = mapped(change);

		assertThat(event.attributes()).isEmpty();
		assertThat(event.summary()).isEmpty();
	}

	@Test
	void skipsOtherWikis() {
		// A namespace 0 edit by a human: only the wiki check keeps it out.
		Skipped skip = skipped(resource("wikidata-edit.json"));

		assertThat(skip.reason()).isEqualTo(SkipReason.OTHER_WIKI);
		assertThat(skip.detail()).isEqualTo("wikidatawiki");
	}

	@Test
	void skipsLogEntriesAndCategoryChanges() {
		// Neither has a revision, which must not make them look malformed.
		assertThat(skipped(resource("enwiki-log.json")).reason()).isEqualTo(SkipReason.NOT_AN_EDIT);
		assertThat(skipped(resource("enwiki-categorize.json")).reason()).isEqualTo(SkipReason.NOT_AN_EDIT);
	}

	@Test
	void skipsNonArticlePages() {
		assertThat(skipped(resource("enwiki-talk-edit.json")).reason()).isEqualTo(SkipReason.NOT_ARTICLE);
	}

	@Test
	void skipsBotEdits() {
		assertThat(skipped(resource("enwiki-bot-edit.json")).reason()).isEqualTo(SkipReason.BOT);
	}

	@Test
	void skipsInvalidJson() {
		assertThat(skipped("{\"wiki\": \"enwiki\", ").reason()).isEqualTo(SkipReason.MALFORMED);
		assertThat(skipped("").reason()).isEqualTo(SkipReason.MALFORMED);
		assertThat(skipped("[]").reason()).isEqualTo(SkipReason.MALFORMED);
	}

	@Test
	void skipsEditWithoutRevision() {
		Skipped skip = skipped(modified("enwiki-edit.json", json -> json.remove("revision")));

		assertThat(skip.reason()).isEqualTo(SkipReason.MALFORMED);
		assertThat(skip.detail()).isEqualTo("missing /revision/new");
	}

	@Test
	void skipsFieldsWithTheWrongType() {
		assertThat(skipped(modified("enwiki-edit.json", json -> json.put("namespace", "0"))).reason())
			.isEqualTo(SkipReason.MALFORMED);
		assertThat(skipped(modified("enwiki-edit.json", json -> json.put("bot", "false"))).reason())
			.isEqualTo(SkipReason.MALFORMED);
		assertThat(skipped(modified("enwiki-edit.json", json -> json.put("timestamp", "1791110083"))).reason())
			.isEqualTo(SkipReason.MALFORMED);
	}

	@Test
	void skipsTimestampOutsideTheRangeOfInstant() {
		assertThat(skipped(modified("enwiki-edit.json", json -> json.put("timestamp", Long.MAX_VALUE))).reason())
			.isEqualTo(SkipReason.MALFORMED);
	}

	@Test
	void skipsMissingWiki() {
		assertThat(skipped(modified("enwiki-edit.json", json -> json.remove("wiki"))).reason())
			.isEqualTo(SkipReason.MALFORMED);
	}

	@Test
	void skipsUrlThatIsNotHttp() {
		String change = modified("enwiki-edit.json",
				json -> ((ObjectNode) json.get("meta")).put("uri", "javascript:alert(1)"));

		assertThat(skipped(change).reason()).isEqualTo(SkipReason.MALFORMED);
	}

	/**
	 * The whole 30-second capture: 859 events from every wiki. The expected counts were worked out
	 * independently from the raw JSON, so this checks the filter order on real traffic.
	 */
	@Test
	void realSampleKeepsOnlyHumanArticleEditsOnEnwiki() {
		List<MappedChange> results = resource("recentchange-sample.jsonl").lines().map(mapper::map).toList();

		Map<SkipReason, Integer> skipped = new EnumMap<>(SkipReason.class);
		results.stream()
			.filter(Skipped.class::isInstance)
			.map(Skipped.class::cast)
			.forEach(skip -> skipped.merge(skip.reason(), 1, Integer::sum));
		List<PulseEvent> events = results.stream()
			.filter(Mapped.class::isInstance)
			.map(result -> ((Mapped) result).event())
			.toList();

		assertThat(results).hasSize(859);
		assertThat(skipped).containsOnly(
				Map.entry(SkipReason.OTHER_WIKI, 781),
				Map.entry(SkipReason.NOT_AN_EDIT, 34),
				Map.entry(SkipReason.NOT_ARTICLE, 7),
				Map.entry(SkipReason.BOT, 13));
		assertThat(events).hasSize(24);
		assertThat(events).extracting(PulseEvent::id).doesNotHaveDuplicates();
		assertThat(events).extracting(PulseEvent::channel).containsOnly("enwiki");
	}

	private PulseEvent mapped(String change) {
		MappedChange result = mapper.map(change);
		assertThat(result).isInstanceOf(Mapped.class);
		return ((Mapped) result).event();
	}

	private Skipped skipped(String change) {
		MappedChange result = mapper.map(change);
		assertThat(result).isInstanceOf(Skipped.class);
		return (Skipped) result;
	}

	private static String modified(String name, Consumer<ObjectNode> change) {
		ObjectNode json = (ObjectNode) new JsonMapper().readTree(resource(name));
		change.accept(json);
		return json.toString();
	}

	static String resource(String name) {
		try (InputStream in = RecentChangeMapperTest.class.getResourceAsStream("/wikipedia/" + name)) {
			return new String(in.readAllBytes(), UTF_8);
		}
		catch (IOException ex) {
			throw new UncheckedIOException(ex);
		}
	}

}
