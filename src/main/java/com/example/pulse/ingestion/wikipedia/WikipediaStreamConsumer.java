package com.example.pulse.ingestion.wikipedia;

import com.example.pulse.event.CheckpointStore;
import com.example.pulse.event.EventAnnotation;
import com.example.pulse.event.EventAnnotations;
import com.example.pulse.event.EventSink;
import com.example.pulse.event.EventSink.Accepted;
import com.example.pulse.ingestion.wikipedia.Disconnect.Reason;
import com.example.pulse.ingestion.wikipedia.MappedChange.Annotated;
import com.example.pulse.ingestion.wikipedia.MappedChange.Mapped;
import com.example.pulse.ingestion.wikipedia.MappedChange.Skipped;
import com.example.pulse.ingestion.wikipedia.StreamTally.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.MissingNode;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Keeps reading Wikipedia's streams on a dedicated thread: connect, map, publish, and reconnect with
 * backoff whenever the connection ends.
 * <p>
 * One connection carries two streams (journal 013). Recent changes become events in the
 * {@link EventSink}; tag changes become {@link EventAnnotation}s, such as "this edit was reverted".
 * Sharing the connection means one thread, one checkpoint, and both streams always resume from the
 * same position; the server's id holds a position per stream.
 * <p>
 * Delivery is at-least-once. The checkpoint is the id of the last event that was fully handled;
 * one thread handles events in order, so everything before it has been published too. It is saved
 * every {@code checkpointInterval} and when a connection ends, not after every event: about 29
 * events a second arrive, and replaying a few seconds after a crash only produces duplicates,
 * which the sink ignores. When publishing fails, the connection is dropped and the next one
 * resumes from the checkpoint, so nothing is skipped.
 * <p>
 * Resuming starts {@link #REPLAY_MARGIN} before the checkpoint, because Wikimedia's ids are
 * timestamps rather than exact positions (see {@link ResumePosition}).
 * <p>
 * Without a checkpoint (the very first start), reading starts from now. Backfilling the stream's
 * history would look like one huge spike.
 */
class WikipediaStreamConsumer {

	/**
	 * The checkpoint's key. It names the first stream only because it existed before the second was
	 * added; renaming it would lose the saved position of running installations.
	 */
	static final String STREAM = "wikipedia.recentchange";

	private static final Logger log = LoggerFactory.getLogger(WikipediaStreamConsumer.class);
	private static final Duration STOP_TIMEOUT = Duration.ofSeconds(10);

	/**
	 * About 150 events at the measured rate, of which about 4 are kept and come back as duplicates.
	 * The largest backwards jump of the ids measured so far was 6 ms.
	 */
	static final Duration REPLAY_MARGIN = Duration.ofSeconds(5);

	private final EventStreamClient client;
	private final JsonMapper json = new JsonMapper();
	private final RecentChangeMapper changeMapper;
	private final TagChangeMapper tagMapper;
	private final EventSink sink;
	private final EventAnnotations annotations;
	private final CheckpointStore checkpoints;
	private final StreamTally tally;
	private final WikipediaMetrics metrics;
	private final Clock clock;
	private final Duration checkpointInterval;
	private final Duration initialBackoff;
	private final Duration maxBackoff;

	private volatile boolean running;
	private Thread thread;

	WikipediaStreamConsumer(EventStreamClient client, RecentChangeMapper changeMapper, TagChangeMapper tagMapper,
			EventSink sink, EventAnnotations annotations, CheckpointStore checkpoints, StreamTally tally,
			WikipediaMetrics metrics, Clock clock, Duration checkpointInterval, Duration initialBackoff,
			Duration maxBackoff) {
		this.client = requireNonNull(client, "client");
		this.changeMapper = requireNonNull(changeMapper, "changeMapper");
		this.tagMapper = requireNonNull(tagMapper, "tagMapper");
		this.sink = requireNonNull(sink, "sink");
		this.annotations = requireNonNull(annotations, "annotations");
		this.checkpoints = requireNonNull(checkpoints, "checkpoints");
		this.tally = requireNonNull(tally, "tally");
		this.metrics = requireNonNull(metrics, "metrics");
		this.clock = requireNonNull(clock, "clock");
		this.checkpointInterval = requireNonNull(checkpointInterval, "checkpointInterval");
		this.initialBackoff = requireNonNull(initialBackoff, "initialBackoff");
		this.maxBackoff = requireNonNull(maxBackoff, "maxBackoff");
	}

	/**
	 * A thread of its own rather than a scheduled task: the stream never ends by design, and it must
	 * never hold up the RSS poller, nor be held up by it.
	 */
	synchronized void start() {
		if (thread != null) {
			throw new IllegalStateException("already started");
		}
		running = true;
		thread = Thread.ofPlatform().name("wikipedia-stream").start(this::run);
	}

	/** Interrupting unblocks both a read in progress and the backoff sleep. */
	synchronized void stop() throws InterruptedException {
		if (thread == null) {
			return;
		}
		running = false;
		thread.interrupt();
		if (!thread.join(STOP_TIMEOUT)) {
			log.warn("wikipedia stream did not stop within {}", STOP_TIMEOUT);
		}
		thread = null;
	}

	synchronized boolean isRunning() {
		return thread != null;
	}

	private void run() {
		Duration backoff = initialBackoff;
		while (running) {
			long handled;
			try {
				handled = connectOnce();
			}
			catch (RuntimeException ex) {
				// A bug must not end ingestion for good; back off and try again.
				log.error("wikipedia stream failed unexpectedly", ex);
				handled = 0;
			}
			if (!running) {
				break;
			}
			// A connection that delivered something was healthy: start over with a short pause.
			backoff = handled > 0 ? initialBackoff : backoff;
			try {
				Thread.sleep(backoff);
			}
			catch (InterruptedException ex) {
				break;
			}
			backoff = min(backoff.multipliedBy(2), maxBackoff);
		}
		log.info("wikipedia stream stopped");
	}

	/** One connection, from loading the checkpoint to the disconnect. Returns the number of events handled. */
	long connectOnce() {
		String position;
		try {
			position = checkpoints.load(STREAM).orElse("");
		}
		catch (RuntimeException ex) {
			log.warn("wikipedia stream: cannot load checkpoint, not connecting ({})", ex.toString());
			return 0;
		}
		log.info("wikipedia stream connecting, {}", position.isEmpty() ? "starting from now" : "resuming");

		Connection connection = new Connection(position);
		Map<WikipediaStream, Snapshot> before = snapshots();
		long start = System.nanoTime();
		Disconnect disconnect = client.read(ResumePosition.rewind(position, REPLAY_MARGIN), connection::handle);
		metrics.disconnected(disconnect.reason(), Duration.ofNanos(System.nanoTime() - start));
		Map<WikipediaStream, Snapshot> handled = new EnumMap<>(WikipediaStream.class);
		snapshots().forEach((stream, now) -> handled.put(stream, now.minus(before.get(stream))));
		if (!running) {
			// Stopping interrupted the read. Clear the flag so the final checkpoint can still be saved.
			Thread.interrupted();
		}
		if (disconnect.reason() == Reason.REJECTED_POSITION) {
			log.error("wikipedia stream: server rejected the saved position, starting from now; events since "
					+ "the last checkpoint are lost ({})", disconnect.detail());
			deleteCheckpoint();
		}
		else {
			connection.saveCheckpoint();
		}
		log(disconnect, handled);
		return handled.values().stream().mapToLong(Snapshot::received).sum();
	}

	private Map<WikipediaStream, Snapshot> snapshots() {
		Map<WikipediaStream, Snapshot> snapshots = new EnumMap<>(WikipediaStream.class);
		for (WikipediaStream stream : WikipediaStream.values()) {
			snapshots.put(stream, tally.snapshot(stream));
		}
		return snapshots;
	}

	private void log(Disconnect disconnect, Map<WikipediaStream, Snapshot> handled) {
		Snapshot changes = handled.get(WikipediaStream.RECENT_CHANGES);
		Snapshot tags = handled.get(WikipediaStream.REVISION_TAGS);
		String message = "wikipedia stream disconnected: {} ({}) | changes: received={} new={} duplicates={} "
				+ "skipped={} {} | tags: received={} annotations new={} known={} skipped={} {}";
		Object[] args = { disconnect.reason(), disconnect.detail(),
				changes.received(), changes.added(), changes.duplicates(), changes.skippedTotal(),
				changes.skippedReasons(),
				tags.received(), tags.added(), tags.duplicates(), tags.skippedTotal(), tags.skippedReasons() };
		// Stopping interrupts the read, which looks like a network error but is expected.
		if (disconnect.reason() == Reason.ENDED || !running) {
			log.info(message, args);
		}
		else {
			log.warn(message, args);
		}
	}

	private void deleteCheckpoint() {
		try {
			checkpoints.delete(STREAM);
		}
		catch (RuntimeException ex) {
			// The next attempt is rejected again and retries the delete.
			log.warn("wikipedia stream: cannot delete checkpoint ({})", ex.toString());
		}
	}

	private static Duration min(Duration a, Duration b) {
		return a.compareTo(b) <= 0 ? a : b;
	}

	/** State of one connection, only touched by the reading thread. */
	private final class Connection {

		private String lastHandledId;
		private String savedId;
		private Instant nextSave;

		Connection(String position) {
			this.lastHandledId = position;
			this.savedId = position;
			this.nextSave = clock.instant().plus(checkpointInterval);
		}

		/** Throws if publishing or saving fails, which ends the connection before this event counts. */
		void handle(ServerSentEvent event) {
			JsonNode data = parse(event.data());
			WikipediaStream stream = WikipediaStream.of(data);
			MappedChange result = stream == WikipediaStream.REVISION_TAGS ? tagMapper.map(data) : changeMapper.map(data);
			switch (result) {
				case Skipped skip -> tally.skipped(stream, skip.reason());
				case Mapped mapped -> {
					boolean isNew = sink.accept(mapped.event()) == Accepted.NEW;
					tally.used(stream, isNew ? 1 : 0, isNew ? 0 : 1);
				}
				case Annotated annotated -> {
					int added = 0;
					for (EventAnnotation annotation : annotated.annotations()) {
						added += annotations.annotate(annotation) ? 1 : 0;
					}
					tally.used(stream, added, annotated.annotations().size() - added);
				}
			}
			if (!event.id().isEmpty()) {
				lastHandledId = event.id();
			}
			Instant now = clock.instant();
			if (!now.isBefore(nextSave)) {
				save();
				nextSave = now.plus(checkpointInterval);
			}
		}

		/** Unparseable data becomes a missing node, which the recent-change mapper rejects as malformed. */
		private JsonNode parse(String data) {
			try {
				return json.readTree(data);
			}
			catch (JacksonException ex) {
				return MissingNode.getInstance();
			}
		}

		/** After a disconnect. Failing is harmless: the next connection replays from the older checkpoint. */
		void saveCheckpoint() {
			try {
				save();
			}
			catch (RuntimeException ex) {
				log.warn("wikipedia stream: cannot save checkpoint ({})", ex.toString());
			}
		}

		private void save() {
			if (!lastHandledId.isEmpty() && !lastHandledId.equals(savedId)) {
				checkpoints.save(STREAM, lastHandledId);
				savedId = lastHandledId;
			}
		}

	}

}
