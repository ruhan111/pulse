package com.example.pulse.ingestion.wikipedia;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import static java.util.Objects.requireNonNull;

/**
 * What the consumer has handled since startup, per stream: the one place it counts. The disconnect
 * log line reads it per connection (via {@link Snapshot#minus}), and {@link WikipediaMetrics} reads
 * it whenever metrics are scraped, so counting never happens twice and metrics are never stale.
 * <p>
 * An event is either skipped or used. A used event produces outputs (an event to store, or
 * annotations), each of which turns out to be new or already known. Written only by the reading
 * thread, read by any thread, hence the atomics. An event whose output failed to store is not
 * counted; it arrives again after the reconnect.
 */
final class StreamTally {

	private final Map<WikipediaStream, Counts> counts = new EnumMap<>(WikipediaStream.class);

	StreamTally() {
		for (WikipediaStream stream : WikipediaStream.values()) {
			counts.put(stream, new Counts());
		}
	}

	void skipped(WikipediaStream stream, SkipReason reason) {
		counts.get(stream).skipped.incrementAndGet(reason.ordinal());
	}

	void used(WikipediaStream stream, int added, int duplicates) {
		Counts counts = this.counts.get(stream);
		counts.used.incrementAndGet();
		counts.added.addAndGet(added);
		counts.duplicates.addAndGet(duplicates);
	}

	long received(WikipediaStream stream) {
		return snapshot(stream).received();
	}

	long skippedCount(WikipediaStream stream, SkipReason reason) {
		return counts.get(stream).skipped.get(reason.ordinal());
	}

	Snapshot snapshot(WikipediaStream stream) {
		Counts counts = this.counts.get(stream);
		Map<SkipReason, Long> skipped = new EnumMap<>(SkipReason.class);
		for (SkipReason reason : SkipReason.values()) {
			skipped.put(reason, counts.skipped.get(reason.ordinal()));
		}
		return new Snapshot(counts.used.get(), counts.added.get(), counts.duplicates.get(), skipped);
	}

	private static final class Counts {

		final AtomicLong used = new AtomicLong();
		final AtomicLong added = new AtomicLong();
		final AtomicLong duplicates = new AtomicLong();
		final AtomicLongArray skipped = new AtomicLongArray(SkipReason.values().length);

	}

	/** The counts of one stream at one moment; the difference of two is what happened in between. */
	record Snapshot(long used, long added, long duplicates, Map<SkipReason, Long> skipped) {

		Snapshot {
			requireNonNull(skipped, "skipped");
			// An EnumMap keeps the log line in enum order.
			skipped = Collections.unmodifiableMap(new EnumMap<>(skipped));
		}

		Snapshot minus(Snapshot earlier) {
			Map<SkipReason, Long> difference = new EnumMap<>(SkipReason.class);
			skipped.forEach((reason, count) -> difference.put(reason, count - earlier.skipped.get(reason)));
			return new Snapshot(used - earlier.used, added - earlier.added, duplicates - earlier.duplicates,
					difference);
		}

		long skippedTotal() {
			return skipped.values().stream().mapToLong(Long::longValue).sum();
		}

		long received() {
			return used + skippedTotal();
		}

		/** Only the reasons that occurred, for a short log line. */
		Map<SkipReason, Long> skippedReasons() {
			Map<SkipReason, Long> occurred = new EnumMap<>(SkipReason.class);
			skipped.forEach((reason, count) -> {
				if (count > 0) {
					occurred.put(reason, count);
				}
			});
			return occurred;
		}

	}

}
