package com.example.pulse.ingestion.wikipedia;

import com.example.pulse.event.CheckpointStore;
import com.example.pulse.event.EventSink;
import com.example.pulse.event.EventSink.Accepted;
import com.example.pulse.event.PulseEvent;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.example.pulse.ingestion.wikipedia.RecentChangeConsumer.STREAM;
import static com.example.pulse.ingestion.wikipedia.StreamServer.events;
import static com.example.pulse.ingestion.wikipedia.StreamServer.eventsThenSilence;
import static com.example.pulse.ingestion.wikipedia.StreamServer.status;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The consumer against a local stream server, with in-memory sink and checkpoints. Uses the real
 * capture: 120 events, of which 2 are kept (revisions 1378416740 and 1378416745).
 */
class RecentChangeConsumerTest {

	private static final String CAPTURE = RecentChangeMapperTest.resource("recentchange-stream.txt");
	private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-04T11:00:00Z"), ZoneOffset.UTC);

	private final StreamServer server = new StreamServer();
	private final RecordingSink sink = new RecordingSink();
	private final InMemoryCheckpoints checkpoints = new InMemoryCheckpoints();
	private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
	private final StreamTally tally = new StreamTally();
	private final WikipediaMetrics metrics = new WikipediaMetrics(registry, tally);

	RecentChangeConsumerTest() throws IOException {
	}

	@AfterEach
	void stopServer() {
		server.close();
	}

	@Test
	void publishesKeptEventsAndSavesTheLastIdWhenTheConnectionEnds() {
		server.then(events(CAPTURE));

		long handled = consumer(Duration.ofHours(1)).connectOnce();

		assertThat(handled).isEqualTo(120);
		assertThat(sink.externalIds()).containsExactly("enwiki:1378416740", "enwiki:1378416745");
		assertThat(checkpoints.load(STREAM)).contains(lastId(CAPTURE));
		assertThat(checkpoints.saves).hasSize(1);
	}

	@Test
	void countsWhatItReceivesSkipsAndHowTheConnectionEnded() {
		server.then(events(CAPTURE));

		consumer(Duration.ofHours(1)).connectOnce();

		assertThat(registry.get("pulse.wikipedia.changes.received").functionCounter().count()).isEqualTo(120);
		assertThat(skipped(SkipReason.OTHER_WIKI)).isEqualTo(105);
		assertThat(skipped(SkipReason.NOT_AN_EDIT)).isEqualTo(10);
		assertThat(skipped(SkipReason.BOT)).isEqualTo(2);
		assertThat(skipped(SkipReason.NOT_ARTICLE)).isEqualTo(1);
		assertThat(skipped(SkipReason.MALFORMED)).isZero();
		assertThat(registry.get("pulse.wikipedia.connections").tag("reason", "ENDED").timer().count()).isEqualTo(1);
	}

	@Test
	void metricsAreCurrentWhileTheConnectionIsStillOpen() throws InterruptedException {
		// The server goes silent after the capture, so nothing ends the connection or triggers a report.
		server.then(eventsThenSilence(CAPTURE));
		RecentChangeConsumer consumer = consumer(Duration.ofHours(1));

		consumer.start();
		awaitEvents(2);
		double received = registry.get("pulse.wikipedia.changes.received").functionCounter().count();
		double otherWiki = registry.get("pulse.wikipedia.changes.skipped").tag("reason", "OTHER_WIKI")
			.functionCounter().count();
		consumer.stop();

		assertThat(received).isEqualTo(120);
		assertThat(otherWiki).isEqualTo(105);
	}

	@Test
	void registersEveryMetricAtZeroBeforeAnythingHappens() {
		// Otherwise Actuator answers 404 for a reason that hasn't occurred yet (journal 008).
		for (SkipReason reason : SkipReason.values()) {
			assertThat(skipped(reason)).isZero();
		}
		for (Disconnect.Reason reason : Disconnect.Reason.values()) {
			assertThat(registry.get("pulse.wikipedia.connections").tag("reason", reason.name()).timer().count()).isZero();
		}
	}

	@Test
	void resumesFromTheSavedCheckpoint() {
		server.then(events(CAPTURE)).then(events(""));
		RecentChangeConsumer consumer = consumer(Duration.ofHours(1));

		consumer.connectOnce();
		consumer.connectOnce();

		assertThat(server.requests().get(0).containsKey("Last-Event-ID")).isFalse();
		assertThat(server.requests().get(1).getFirst("Last-Event-ID")).isEqualTo(rewound(lastId(CAPTURE)));
	}

	@Test
	void savesPeriodicallyWhileReading() {
		server.then(events(CAPTURE));

		// A zero interval saves after every event that moves the position. Events can share an id.
		consumer(Duration.ZERO).connectOnce();

		assertThat(checkpoints.saves).hasSize(118).doesNotHaveDuplicates();
	}

	@Test
	void aFailedPublishDoesNotAdvanceTheCheckpointPastTheFailedEvent() {
		server.then(events(CAPTURE)).then(events(CAPTURE));
		sink.failOn("enwiki:1378416745");
		RecentChangeConsumer consumer = consumer(Duration.ZERO);

		consumer.connectOnce();
		String resumedFrom = checkpoints.load(STREAM).orElseThrow();
		sink.failOn(null);
		consumer.connectOnce();

		// The checkpoint is the event just before the failed one, so the retry starts with it.
		assertThat(resumedFrom).isEqualTo(idBefore(CAPTURE, "1378416745"));
		assertThat(server.requests().get(1).getFirst("Last-Event-ID")).isEqualTo(rewound(resumedFrom));
		// The local server replays from the start, so the first event comes twice: harmless, it's a duplicate.
		assertThat(sink.externalIds()).containsExactly("enwiki:1378416740", "enwiki:1378416740", "enwiki:1378416745");
	}

	@Test
	void forgetsAPositionTheServerRejectsAndStartsFromNow() {
		checkpoints.save(STREAM, "garbage");
		server.then(status(400)).then(events(""));
		RecentChangeConsumer consumer = consumer(Duration.ofHours(1));

		consumer.connectOnce();
		consumer.connectOnce();

		assertThat(checkpoints.load(STREAM)).isEmpty();
		assertThat(server.requests().get(1).containsKey("Last-Event-ID")).isFalse();
	}

	@Test
	void doesNotConnectWhenTheCheckpointCannotBeLoaded() {
		checkpoints.failing = true;

		assertThat(consumer(Duration.ofHours(1)).connectOnce()).isZero();
		assertThat(server.requests()).isEmpty();
	}

	@Test
	void reconnectsInTheBackgroundAndStopsPromptlyMidStream() throws InterruptedException {
		server.then(status(503)).then(eventsThenSilence(CAPTURE));
		RecentChangeConsumer consumer = consumer(Duration.ofHours(1));

		consumer.start();
		awaitEvents(2);
		long start = System.nanoTime();
		consumer.stop();
		Duration stopping = Duration.ofNanos(System.nanoTime() - start);

		assertThat(server.requests()).hasSize(2);
		assertThat(stopping).isLessThan(Duration.ofSeconds(2));
		// Stopping still saves where it got to.
		assertThat(checkpoints.load(STREAM)).contains(lastId(CAPTURE));
	}

	private RecentChangeConsumer consumer(Duration checkpointInterval) {
		EventStreamClient client = new EventStreamClient(server.url(), Duration.ofSeconds(5), Duration.ofMinutes(1));
		return new RecentChangeConsumer(client, new RecentChangeMapper(CLOCK), sink, checkpoints, tally, metrics,
				CLOCK, checkpointInterval, Duration.ofMillis(10), Duration.ofMillis(100));
	}

	private double skipped(SkipReason reason) {
		return registry.get("pulse.wikipedia.changes.skipped").tag("reason", reason.name()).functionCounter().count();
	}

	private void awaitEvents(int count) throws InterruptedException {
		long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
		while (sink.events.size() < count && System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		// The last kept event is followed by a few skipped ones before the server goes silent.
		Thread.sleep(200);
	}

	private static String rewound(String id) {
		return ResumePosition.rewind(id, RecentChangeConsumer.REPLAY_MARGIN);
	}

	private static String lastId(String capture) {
		return ids(capture).getLast();
	}

	private static String idBefore(String capture, String revision) {
		List<String> ids = ids(capture);
		List<String> data = capture.lines().filter(line -> line.startsWith("data: ")).toList();
		for (int i = 0; i < data.size(); i++) {
			if (data.get(i).contains("\"new\":" + revision + "}")) {
				return ids.get(i - 1);
			}
		}
		throw new AssertionError("revision " + revision + " not in capture");
	}

	private static List<String> ids(String capture) {
		return capture.lines().filter(line -> line.startsWith("id: ")).map(line -> line.substring(4)).toList();
	}

	private static final class RecordingSink implements EventSink {

		final List<PulseEvent> events = new CopyOnWriteArrayList<>();
		private final Set<String> seen = new HashSet<>();
		private volatile String failOn;

		void failOn(String externalId) {
			failOn = externalId;
		}

		List<String> externalIds() {
			return events.stream().map(PulseEvent::externalId).toList();
		}

		@Override
		public synchronized Accepted accept(PulseEvent event) {
			if (event.externalId().equals(failOn)) {
				throw new IllegalStateException("database down");
			}
			events.add(event);
			return seen.add(event.externalId()) ? Accepted.NEW : Accepted.DUPLICATE;
		}

	}

	private static final class InMemoryCheckpoints implements CheckpointStore {

		final List<String> saves = new CopyOnWriteArrayList<>();
		private final Map<String, String> positions = new ConcurrentHashMap<>();
		volatile boolean failing;

		@Override
		public Optional<String> load(String stream) {
			if (failing) {
				throw new IllegalStateException("database down");
			}
			return Optional.ofNullable(positions.get(stream));
		}

		@Override
		public void save(String stream, String position) {
			saves.add(position);
			positions.put(stream, position);
		}

		@Override
		public void delete(String stream) {
			positions.remove(stream);
		}

	}

}
