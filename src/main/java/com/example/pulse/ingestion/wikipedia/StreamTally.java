package com.example.pulse.ingestion.wikipedia;

import com.example.pulse.event.EventSink.Accepted;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import static java.util.Objects.requireNonNull;

/**
 * What the stream has handled since startup: the one place the consumer counts. The disconnect log
 * line reads it per connection (via {@link Snapshot#minus}), and {@link WikipediaMetrics} reads it
 * whenever metrics are scraped, so counting never happens twice and metrics are never stale.
 * <p>
 * Written only by the reading thread, read by any thread, hence the atomics. A change counts once it
 * has been handled: skipped, or accepted by the sink. One that failed to publish is not counted; it
 * arrives again after the reconnect.
 */
final class StreamTally {

	private final AtomicLong newEvents = new AtomicLong();
	private final AtomicLong duplicates = new AtomicLong();
	private final AtomicLongArray skipped = new AtomicLongArray(SkipReason.values().length);

	void skipped(SkipReason reason) {
		skipped.incrementAndGet(reason.ordinal());
	}

	void accepted(Accepted accepted) {
		switch (accepted) {
			case NEW -> newEvents.incrementAndGet();
			case DUPLICATE -> duplicates.incrementAndGet();
		}
	}

	long received() {
		long received = newEvents.get() + duplicates.get();
		for (int i = 0; i < skipped.length(); i++) {
			received += skipped.get(i);
		}
		return received;
	}

	long skippedCount(SkipReason reason) {
		return skipped.get(reason.ordinal());
	}

	Snapshot snapshot() {
		Map<SkipReason, Long> bySkipReason = new EnumMap<>(SkipReason.class);
		for (SkipReason reason : SkipReason.values()) {
			bySkipReason.put(reason, skippedCount(reason));
		}
		return new Snapshot(newEvents.get(), duplicates.get(), bySkipReason);
	}

	/** The counts at one moment; the difference of two is what happened in between. */
	record Snapshot(long newEvents, long duplicates, Map<SkipReason, Long> skipped) {

		Snapshot {
			requireNonNull(skipped, "skipped");
			// An EnumMap keeps the log line in enum order.
			skipped = Collections.unmodifiableMap(new EnumMap<>(skipped));
		}

		Snapshot minus(Snapshot earlier) {
			Map<SkipReason, Long> difference = new EnumMap<>(SkipReason.class);
			skipped.forEach((reason, count) -> difference.put(reason, count - earlier.skipped.get(reason)));
			return new Snapshot(newEvents - earlier.newEvents, duplicates - earlier.duplicates, difference);
		}

		long skippedTotal() {
			return skipped.values().stream().mapToLong(Long::longValue).sum();
		}

		long received() {
			return newEvents + duplicates + skippedTotal();
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
