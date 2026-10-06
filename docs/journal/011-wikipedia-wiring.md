# 011: Running the Wikipedia stream in the app (Phase 4c)

**Date:** 2026-10-06 · **Phase:** 4c · **Commit/PR:** Phase 4c PR

## Context

4a (009) maps a recent change and 4b (010) reads the stream with resumable checkpoints, but
nothing ran in the application yet. 4c wires the consumer into Spring, adds metrics, and runs the
whole app against the live stream. The run includes a deliberate app outage and a database
outage. 010 left one question open: how fast does Pulse catch up after being down?

## Decision

- **`WikipediaConfiguration`** is the only public entry point into `ingestion.wikipedia`, like
  `RssConfiguration`. It's active when `pulse.wikipedia.enabled=true`.
- **A `SmartLifecycle` bean starts and stops the consumer.**
  - It starts after the context is fully refreshed, so the Liquibase migrations have run.
  - It stops in the lifecycle phase of shutdown, which comes before singletons (the Hikari pool)
    are destroyed. That ordering is what lets the final checkpoint be saved on shutdown.
  - `RecentChangeConsumer` itself stays free of Spring. It only gained `isRunning()`.
- **`WikipediaProperties`** (`pulse.wikipedia.*`): `stream-url`, `connect-timeout` (10 s),
  `idle-timeout` (30 s), `checkpoint-interval` (5 s). As with RSS, a missing setting stops startup.
  Backoff (1 s → 60 s) and the 5 s replay margin stay constants, because nothing has needed other
  values.
- **`WikipediaMetrics`**:
  - `pulse.wikipedia.changes.received`: everything from the stream, before filtering.
  - `pulse.wikipedia.changes.skipped` (`reason`).
  - `pulse.wikipedia.connections` (`reason`): a timer, so it counts disconnects *and* records how
    long each connection lasted.
  - New vs. duplicate is already counted by the sink (`pulse.events.accepted`,
    `source=WIKIPEDIA`).
  - **Every meter is registered at 0 on startup.** This fixes the 404 confusion from 008 for these
    metrics: Actuator answers 404 for a tag value until its counter exists.
- **Tests stay off the internet.** Five tests start the whole app and load the real
  `application.yaml`, which now enables the stream. They set `pulse.wikipedia.enabled=false`, as
  they already did for RSS.

## Alternatives considered

- **`RecentChangeConsumer implements SmartLifecycle`.** That's shorter, but it would tie the
  consumer to Spring. `RssPoller` is Spring-free too, with its scheduling in the configuration.
- **`@PostConstruct`/`@PreDestroy`.** `@PostConstruct` runs during bean creation, before the context
  is complete. A `@PreDestroy` would run in the same destruction pass as the pool, with no ordering
  guarantee. `SmartLifecycle` gives exactly "after startup" and "before teardown".
- **A test `application.yaml` that disables the stream.** It would shadow the main one on the
  classpath, so tests would stop checking the shipped configuration. Explicit properties are
  clearer.
- **Making backoff configurable.** That's configuration nobody needs yet.
- **A lag gauge** (how far behind the reader is). It would need the stream position as a time,
  i.e. parsing ids outside `ResumePosition`. `pulse.events.lateness` already shows it for kept
  events, and it did show the outage below (315 s).

## What happened

- **Tests:**
  - **`WikipediaIngestionEndToEndTest`** runs the whole app twice against a local stream server
    replaying the real capture, with PostgreSQL in Testcontainers. The first run stores the 2 kept
    changes and serves the metrics over HTTP, including a 0 for `MALFORMED`. Shutdown saves the
    position. The restart resumes from it with the 5 s rewind. The replay produces 2 duplicates and
    no new rows.
  - **`WikipediaConfigurationTest`** checks the shipped settings, that the app fails to start
    without a setting or without a `CheckpointStore`, and that the consumer starts and stops with
    the context.
- **Proving the tests** (each restored afterwards, then green):

| Break | Red |
|---|---|
| No checkpoint save when a connection ends | the end-to-end test: the restart sent no `Last-Event-ID` |
| The lifecycle bean removed | the end-to-end test (nothing stored) and `startsTheStreamWithTheContextAndStopsItOnClose` |
| Skip counters created on first use instead of at startup | the end-to-end test (404 for `MALFORMED`) and both metric tests in `RecentChangeConsumerTest` |

- **A real run in the sandbox.**
  - Setup: the packaged jar, `compose.yaml` PostgreSQL, the live stream through the sandbox proxy,
    and RSS off (its feeds aren't reachable from here).
  - A script sampled `/actuator/metrics`: every minute for 20 min, then the app stopped for 5 min
    and was restarted and sampled every 5 s, then PostgreSQL was stopped for 62 s. Numbers below.
- **The far side closed a healthy connection after 10 min 40 s.** It showed up as `NETWORK_ERROR
  (IOException: closed)`, without the idle flag. The consumer reconnected 1 s later from its
  checkpoint, and the replay margin produced 12 duplicates. The later connection lasted 10 min 15 s
  without this happening, until the app was stopped. I can't tell yet whether Wikimedia or the
  sandbox's proxy closed it. The `connections` timer will show a pattern on a longer run.
- **During the database outage, reading simply stopped.**
  - `received` froze at 29,040 for the whole outage. The reader thread waits inside
    `sink.accept()`, and TCP flow control holds the stream back.
  - Hikari found all 10 pooled connections closed (10 warnings in the same millisecond).
  - The publish failed 61 s after the stop, as PostgreSQL was coming back. That's about twice
    the 30 s connection timeout; I haven't looked into why.
  - Result: `HANDLER_FAILED`, and the final checkpoint save failed too (logged). The checkpoint
    stayed before the failed event.
- **Wikimedia answered HTTP 500 once** on the first reconnect after the database outage
  (21:06:00), unrelated to Pulse. Backoff 2 s, and the next attempt worked. It was the first real
  `HTTP_ERROR` seen, and handled as designed.
- **No holes.** Per minute of `occurred_at`, the stored counts were:
  - **App outage** (20:53:34–20:58:43): 73, 68, 72, 78, 89 for minutes 20:54–20:58.
  - **Database outage** (21:04:52–21:05:54): 73 for minute 21:05.
  - **Minutes around them:** 58–91.

  Both gaps were filled in by the resume.
- **The checkpoint's format can change.** Earlier ids had a timestamp only for `eqiad` and
  `"offset":-1` for `codfw`. After the HTTP 500 and reconnect, the saved id carried timestamps for
  both, with `codfw` 150 s older:
  `[{"topic":"codfw…","timestamp":1791320788497},{"topic":"eqiad…","timestamp":1791320938339}]`.
  `ResumePosition` rewinds every timestamp it finds, so it handles this.
- **The `new event …` log line is now the bulk of the log.**
  - Run 1: 1,584 such lines in 20 minutes, and the log grew by 370 kB (about 1.1 MB/hour).
  - With RSS (12 events/hour) the line was useful. At 4,700/hour it drowns everything else.
  - I left it unchanged: two tests assert it, and changing what the sink logs is a separate
    decision. See open questions.

## Measurements

The sandbox run, Tuesday 2026-10-06, 20:33–21:09 UTC, one app instance, PostgreSQL 17 in Docker on
the same machine.

**Steady state** (20:33:29–20:53:34, 1,205 s):

| Measure | Value |
|---|---:|
| Changes received | 46,726 (**38.8/s**) |
| Stored as new | 1,583 (**1.31/s, ≈ 4,700/hour**) |
| Skipped `OTHER_WIKI` | 42,331 (90.6% of received) |
| Duplicates (one far-side disconnect) | 12 |
| Insert max, first minute (warm-up) / later | 77.5 ms / 22.2 ms |
| `process.cpu.usage` (share of the whole machine) | 0.6–0.7% |
| Heap used | 32–99 MB, sawtooth |
| Hikari active connections at each sample | 0 |

**Catching up after 5 min 9 s down:**

| Measure | Value |
|---|---:|
| Backlog (309 s down + 5 s margin, at 38.8/s) | ≈ 12,200 changes |
| Received ≤ 6 s after reconnecting | 13,287 |
| **Catch-up speed** | **≥ 2,000 changes/s** (a lower bound: the backlog was done before the first 5 s sample) |
| Stored during catch-up | 405 new, 7 duplicates |
| `process.cpu.usage` during catch-up sample | 33% |
| Max lateness of stored events | 315 s (the outage, as expected) |

**Database down for 62 s:**

| Measure | Value |
|---|---:|
| Reading stopped | at once (received constant for 61 s) |
| Time until the publish failed | 61 s |
| Back to reading after PostgreSQL started | 5 s (plus one Wikimedia 500, 2 s) |
| Duplicates from the resume | 13 |
| Gap in stored data | none (per-minute counts above) |

**Storage after the run:** 2,721 Wikipedia rows, 944 kB including index and TOAST, or about 355
bytes per row (average `pg_column_size` 276 bytes, average summary 30 characters).
- At about 4,700 rows/hour that's about 40 MB a day, or 15 GB a year.
- The metrics counted 2,719 new events. The 2 missing ones were stored between the last sample and
  each stop.

**Rates so far** (all English Wikipedia, kept after filters):

| When (UTC) | Received/s | Kept/s | Source |
|---|---:|---:|---|
| Sun 10:34, 30 s | 29 | 0.8 | 009 |
| Sun 11:05, 60 s | 47 | 1.0 | 010 |
| Tue 20:33, 20 min | 38.8 | 1.31 | this entry |

**Test suite:** 132 tests (9 new), all passing, `./mvnw clean test`.

## Consequences / open questions

- **Phase 4's goal is met.** Pulse ingests a source with about 400× the RSS rate (4,700 vs. 12
  events/hour), survives disconnects, app restarts and database outages without gaps, and shows
  all of it in metrics.
- **What 4d was waiting for, answered by these numbers:**
  - **Batching inserts: not needed.** About 1.3 inserts a second, with an idle pool at every
    sample.
  - **Decoupling ingestion from storage: not needed yet.** A database outage pauses reading and
    the resume fills the gap; nothing is lost. The cost is that Wikipedia data is late by the
    length of the outage. That's acceptable at this scale.
  - **Retention: not urgent.** It's about 40 MB a day.
  - **Catch-up after an outage is fast.** At ≥ 2,000 changes/s, a whole day's backlog (about 3.4
    million changes) would take under half an hour, if that speed holds for a longer backlog.
    That's untested.

  So there is no evidence for new infrastructure. I propose closing Phase 4 here and moving to
  Phase 5 (trend detection), rather than inventing a 4d.
- **Decision needed: the `new event` log line.** Options:
  - **(a)** Log it at debug, and update the two tests that count it. They could count rows instead.
  - **(b)** Keep info for RSS only. But the sink doesn't know the source's rate, so that would be a
    per-source rule in the wrong place.
  - **(c)** Replace it with a periodic summary line.

  I'd pick (a).
- **Still open from earlier:**
  - A multi-hour run on my machine for the daily rhythm and the far-side disconnect pattern (the
    `connections` timer now records both).
  - Counters at 0 for the RSS and sink metrics (only Wikipedia's are fixed).
  - Hacker News timeouts (008).
- **Phase 5 has to count Wikipedia by `occurredAt`.** The catch-up stored events up to 315 s late,
  and a longer outage would make that hours.
