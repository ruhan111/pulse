package com.example.pulse.ingestion.wikipedia;

import com.example.pulse.event.EventAnnotation;
import com.example.pulse.event.EventAnnotation.Kind;
import com.example.pulse.event.EventId;
import com.example.pulse.event.Source;
import com.example.pulse.ingestion.wikipedia.MappedChange.Annotated;
import com.example.pulse.ingestion.wikipedia.MappedChange.Skipped;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every fixture is a real event from the {@code mediawiki.revision-tags-change} stream, recorded on
 * 2026-10-08 together with the recent changes in {@code two-streams.txt}.
 */
class TagChangeMapperTest {

	private static final JsonMapper JSON = new JsonMapper();

	private final TagChangeMapper mapper = new TagChangeMapper();

	@Test
	void anUndoneEditIsAnnotatedAsReverted() {
		// Torrington, Connecticut: the edit that was rolled back.
		assertThat(annotations("tags-reverted.json")).containsExactly(new EventAnnotation(
				EventId.of(Source.WIKIPEDIA, "enwiki:1379235224"), Kind.REVERTED,
				Instant.parse("2026-10-08T19:17:41.409Z")));
	}

	@Test
	void rollbackUndoAndManualRevertAreAllReverts() {
		assertThat(kinds("tags-rollback.json")).containsExactly(Kind.REVERT);
		assertThat(kinds("tags-undo.json")).containsExactly(Kind.REVERT);
		// No summary says so; MediaWiki noticed the content went back to an earlier version.
		assertThat(kinds("tags-manual-revert.json")).containsExactly(Kind.REVERT);
	}

	@Test
	void aNewRedirectIsAnnotated() {
		assertThat(kinds("tags-new-redirect.json")).containsExactly(Kind.REDIRECT);
	}

	@Test
	void pointsAtTheSameEventAsTheRecentChangeOfThatRevision() {
		// The rollback in the two-stream capture: revision 1379235231, mapped by both mappers.
		String change = RecentChangeMapperTest.resource("two-streams.txt").lines()
			.filter(line -> line.startsWith("data: ") && line.contains("\"new\":1379235231}"))
			.findFirst().orElseThrow().substring(6);
		MappedChange mapped = new RecentChangeMapper(Clock.systemUTC()).map(change);

		assertThat(mapped).isInstanceOfSatisfying(MappedChange.Mapped.class,
				event -> assertThat(annotations("tags-rollback.json").getFirst().eventId()).isEqualTo(event.event().id()));
	}

	@Test
	void skipsTagsPulseDoesNotUse() {
		assertThat(skipped(json("tags-editor-only.json")).reason()).isEqualTo(SkipReason.NO_RELEVANT_TAG);
	}

	@Test
	void skipsOtherWikisAndNonArticlePages() {
		assertThat(skipped(json("tags-other-wiki.json")).reason()).isEqualTo(SkipReason.OTHER_WIKI);
		assertThat(skipped(json("tags-talk-page.json")).reason()).isEqualTo(SkipReason.NOT_ARTICLE);
	}

	@Test
	void onlyNewlyAddedTagsCount() {
		// The event repeats tags reported before; an old "reverted" must not be recorded again.
		JsonNode alreadyKnown = modified("tags-reverted.json", tags -> {
			ArrayNode prior = ((ObjectNode) tags.get("prior_state")).putArray("tags");
			prior.add("mw-reverted");
		});

		assertThat(skipped(alreadyKnown).reason()).isEqualTo(SkipReason.NO_RELEVANT_TAG);
	}

	@Test
	void worksWithoutPriorState() {
		JsonNode noPriorState = modified("tags-reverted.json", tags -> tags.remove("prior_state"));

		assertThat(mapper.map(noPriorState)).isInstanceOf(Annotated.class);
	}

	@Test
	void oneChangeCanAddSeveralKinds() {
		JsonNode both = modified("tags-reverted.json", tags -> ((ArrayNode) tags.get("tags")).add("mw-new-redirect"));

		assertThat(((Annotated) mapper.map(both)).annotations()).extracting(EventAnnotation::kind)
			.containsExactly(Kind.REVERTED, Kind.REDIRECT);
	}

	@Test
	void skipsMalformedChanges() {
		assertThat(skipped(modified("tags-reverted.json", tags -> tags.remove("rev_id"))).detail())
			.isEqualTo("missing /rev_id");
		assertThat(skipped(modified("tags-reverted.json", tags -> ((ObjectNode) tags.get("meta")).put("dt", "yesterday")))
			.reason()).isEqualTo(SkipReason.MALFORMED);
		assertThat(skipped(JSON.readTree("{}")).reason()).isEqualTo(SkipReason.MALFORMED);
	}

	private List<EventAnnotation> annotations(String fixture) {
		MappedChange result = mapper.map(json(fixture));
		assertThat(result).isInstanceOf(Annotated.class);
		return ((Annotated) result).annotations();
	}

	private List<Kind> kinds(String fixture) {
		return annotations(fixture).stream().map(EventAnnotation::kind).toList();
	}

	private Skipped skipped(JsonNode change) {
		MappedChange result = mapper.map(change);
		assertThat(result).isInstanceOf(Skipped.class);
		return (Skipped) result;
	}

	private static JsonNode json(String fixture) {
		return JSON.readTree(RecentChangeMapperTest.resource(fixture));
	}

	private static JsonNode modified(String fixture, Consumer<ObjectNode> change) {
		ObjectNode json = (ObjectNode) json(fixture);
		change.accept(json);
		return json;
	}

}
