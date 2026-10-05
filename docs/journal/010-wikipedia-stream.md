# 010: Reading the Wikipedia stream (Phase 4b)

**Date:** 2026-10-04 · **Phase:** 4b · **Commit/PR:** Phase 4b PR

## Context

4a (009) maps one recent change to a `PulseEvent`. 4b connects to Wikimedia's EventStreams, keeps
reading, survives disconnects and restarts, and resumes where it stopped. It is not wired into
Spring yet. Settings, metrics and a long real run are 4c.

Decisions agreed before coding:

- **The resume position is stored in PostgreSQL**, behind a port in `event`.
- **A rejected position is forgotten**, and reading starts from now.
- **No backfill** on the very first start.
- **After an outage, everything is replayed.**

## Decision

New code:

| Where | What |
|---|---|
| `event` | **`CheckpointStore`** port: `load` / `save` / `delete` an opaque position per stream |
| `infrastructure` | **`JdbcCheckpointStore`**, changeset `002-create-checkpoints-table.xml`: `checkpoints(stream pk, position, updated_at)`; SQL files `find_checkpoint`, `upsert_checkpoint`, `delete_checkpoint` |
| `ingestion.wikipedia` | **`SseParser`** and **`ServerSentEvent`**: the HTML spec's event-stream format |
| | **`EventStreamClient`**: one HTTP connection, read until it ends; returns a **`Disconnect`** with an enum reason |
| | **`RecentChangeConsumer`**: the reconnect loop on its own thread, checkpoints, backoff |
| | **`ResumePosition`**: rewinds a saved id before resuming (see *What happened*) |

- **The port lives in `event`, next to `EventSink`.** `ArchitectureTest` forbids anything outside
  `ingestion` from depending on it, so `infrastructure` can't implement an interface declared in
  `ingestion.wikipedia`. The architecture rules stay unchanged.
- **Positions are opaque strings.** The store saves whatever the source sent and returns it
  unchanged. Here a checkpoint is an upsert (`on conflict do update`), unlike events, where the
  first version wins.
- **SSE parsing is our own, about 60 lines.** The JDK has no SSE client. The parser follows the
  spec:
  - `data:` lines are joined with `\n`
  - `:` comments are ignored (Wikimedia starts with `:ok`)
  - one leading space after the colon is stripped
  - the `id` carries over to later events that have none
  - `retry:` is ignored
  - an event cut off by the end of the stream is never dispatched

  Line splitting is `BufferedReader.readLine`, which accepts CR, LF and CRLF as the spec requires.
- **One connection = one call.** `EventStreamClient.read(lastEventId, handler)` hands each event to
  the handler on the calling thread and returns why the connection ended: `ENDED`,
  `NETWORK_ERROR`, `HTTP_ERROR`, `REJECTED_POSITION` (400 to a request with a `Last-Event-ID`),
  `IDLE_TIMEOUT`, or `HANDLER_FAILED`.
- **An idle watchdog.** The request timeout only covers waiting for headers. Once the body flows,
  `HttpClient` has no read timeout, and a silently dead connection would block forever. A watchdog
  thread closes the body after 30 s without a line, which makes the blocked read fail. Time spent
  in the handler doesn't count, because a slow database is not a dead connection.
- **The consumer** runs on its own platform thread (`wikipedia-stream`), so it can't hold up the
  RSS poller or be held up by it.
  - It loads the checkpoint, connects, maps, publishes, and reconnects.
  - Backoff starts at 1 s, doubles to at most 60 s, and resets after a connection that delivered
    events.
  - If the checkpoint can't be loaded (database down), it doesn't connect, and backs off.
  - `stop()` interrupts the thread, which unblocks both a read and the backoff sleep. It then
    still saves the final checkpoint.
- **Checkpoints are at-least-once.**
  - The checkpoint is the id of the last *fully handled* event.
  - It is saved every 5 s and when a connection ends, not after every event (29–47 events/s
    arrive).
  - When publishing fails, the connection is dropped. The checkpoint never moves past the failed
    event, and the next connection replays from it.
- **`User-Agent: Pulse/0.1 (+https://github.com/ruhan111/pulse)`.** It's the same string as the RSS
  fetcher, duplicated as a constant rather than shared: the two adapters must not depend on each
  other, and a shared `ingestion.http` package isn't justified by one constant.

No new dependencies.

## Alternatives considered

- **Loosening `ArchitectureTest` so `infrastructure` may depend on `ingestion`.** Infrastructure
  would then know about individual sources. A port in `event` costs one interface.
- **Deriving the resume point from the `events` table** (the newest stored Wikipedia event). This
  needs a read port anyway, and it ties resuming to what was *kept* (3%) rather than what was
  *read*.
- **Saving the checkpoint after every event.** That's 29–47 writes a second for about one kept
  event a second. A crash replays at most 5 s, which is harmless.
- **`@Scheduled` or a virtual thread.** The stream never ends by design. One long-lived platform
  thread with a name is the simplest thing that shows up clearly in a thread dump.
- **An SSE library** (e.g. OkHttp's `okhttp-sse`). It would bring a second HTTP client for about
  60 lines of parsing.
- **`?since=<timestamp>` instead of `Last-Event-ID`.** Both work. `Last-Event-ID` is the SSE
  standard and keeps the client generic. The position format stays the server's business, apart
  from the rewind below.
- **Building our own exact position from each event's `meta.offset`** (Kafka offsets are contiguous;
  see below). It would be exact, but it needs the active datacenter's topic name and breaks on a
  datacenter switchover. The rewind is simpler and only costs duplicates.

## What happened

- **Library behaviour, checked directly before relying on it** (throwaway program, local `HttpServer`):
  - Closing the response `InputStream` from another thread makes a blocked `readLine` throw
    `IOException: closed` within about 1 ms. This is the watchdog's mechanism.
  - Interrupting the reading thread makes the read throw `IOException(InterruptedException)`.
    This is how `stop()` works.
  - The JDK `HttpServer`'s default executor is **a single thread**. My first experiment hung,
    because the second request waited behind the first stalled one. The test server
    (`StreamServer`) uses a thread pool.
- **The real stream, probed by hand:**
  - `Last-Event-ID` resumes. A timestamp from 10 minutes ago started exactly 10 minutes back.
  - History reaches back at least 8 days.
  - A malformed `Last-Event-ID` gets **400**. No `User-Agent` gets **403**. The connection is HTTP/2.
- **Wikimedia's event ids are timestamps, not positions, and that loses events.** An id looks like
  `[{"topic":"eqiad.mediawiki.recentchange","partition":0,"timestamp":1791111567327},
  {"topic":"codfw…","offset":-1}]`. I found two problems on the live stream:
  - **Ids are not unique.** In four captures (1,685 events, two of them partly overlapping), 37
    events shared their id with the event before (about 2%). I resumed from an id shared by the events at Kafka offsets 6581725992 and
    6581725993. The stream continued at **6581725994**: the server resumes *after* every event in
    that millisecond. If a crash or a failed publish falls between two such events, the second
    one is lost.
  - **Ids sometimes go backwards**, 16 times in those captures, by 1–6 ms. An event that
    arrives after the checkpoint but carries an earlier timestamp would also be skipped.
  - The Kafka offsets in `meta.offset` were perfectly contiguous in every capture, so the data
    itself is in order. Only the ids are coarse.
  - **Fix: `ResumePosition` rewinds every timestamp in the id by 5 s before resuming.** Resuming
    from the shared id rewound by only 1 s started at offset 6581725967, 25 events early, and
    included both events of the pair. 5 s is about 1,000× the largest backwards jump seen, and
    costs about 150 replayed stream events per reconnect, of which a few are kept and stored as
    duplicates. The stored checkpoint stays exactly what the server sent. Anything that isn't
    understood is passed through unchanged, and the server rejects it if it's malformed.
- **Proving the tests** (each break restored afterwards, then green):

| Break | Test that went red |
|---|---|
| The watchdog never closes | `closesASilentConnectionAfterTheIdleTimeout` |
| The watchdog counts handler time | `aSlowHandlerIsNotADeadConnection` |
| The position advances before publishing | `aFailedPublishDoesNotAdvanceTheCheckpointPastTheFailedEvent` |
| No rewind | `ResumePositionTest.movesTimestampsBackAndKeepsTheServersFormat` |
| Checkpoint `do nothing` instead of upsert | `JdbcCheckpointStoreTest.theLatestPositionWins` |

  With no rewind, the consumer tests stayed green. They compute their expected `Last-Event-ID`
  with `ResumePosition` itself, so `ResumePositionTest` is the guard there.
- **A deliberate stop logged a warning.** Interrupting the read surfaces as `NETWORK_ERROR`, so
  every shutdown logged `WARN … NETWORK_ERROR (IOException: InterruptedException)`. It's now
  logged at info when the consumer is stopping.
- **Correction to 009:** it reported 88 tests, but the real count was 87. A throwaway `ScratchTest`
  had been deleted from `src/`, but its compiled class was still in `target/test-classes`, and
  Surefire ran it. This entry's numbers come from `./mvnw clean test`.

## Measurements

**A live smoke run in the sandbox** (throwaway test, not committed): the consumer against the real
stream through the sandbox's proxy, in-memory sink and checkpoints, 2026-10-04 around 11:05 UTC.
- Ran 60 s, stopped, stayed down 20 s, then resumed for 15 s.

| Measure | Value |
|---|---:|
| Run 1: stream events in 60 s | 2,830 (**47/s**) |
| Run 1: kept | 58 (≈ 1/s, ≈ 3,500/hour) |
| Checkpoint saved on stop | yes |
| Run 2: stream events in 15 s (25 s backlog: 20 s down + 5 s margin, then live) | 2,326 |
| Run 2: kept, new | 31 |
| Run 2: duplicates (replay margin) | 9 |

- **Catching up works, but this run doesn't show how fast.** The 2,326 events are the backlog plus
  15 s of live traffic, so dividing by 15 s gives no catch-up speed. 4c should measure it, with a
  deliberate outage, because it decides how long a restart takes to become current again.
- **9 duplicates is more than the 5 s margin predicts at about 1 kept event a second (about 5).** I
  haven't looked into why: bursts, or edits in the stream arriving with their own delay.
  Duplicates are harmless, but 4c's metrics will count them.
- **The rate varies a lot:** 29 events/s in 009's sample (10:34 UTC) and 47/s here (11:05 UTC). One
  more reason for 4c's long run.
- **Test suite:** 123 tests (36 new), all passing, `./mvnw clean test`.

## Consequences / open questions

- Nothing runs in the app yet. **4c** wires `RecentChangeConsumer` into Spring:
  - settings under `pulse.wikipedia` (enabled, URL, timeouts)
  - start and stop with the application context
  - metrics for disconnect reasons, skip reasons, events per second and replay duplicates
  - a long real run on my machine
- **The resume logic depends on Wikimedia's id format.** If the ids change, `ResumePosition` passes
  them through unchanged. Resuming then still works, just without the margin. A test pins the
  current format.
- **Replaying after a long outage:** a day down means re-reading about 2.5–4 million stream events
  (29–47/s). How long that takes is unknown until 4c measures the catch-up speed. Meanwhile,
  events arrive with old `occurredAt` and fresh `ingestedAt`, so Phase 5 has to count Wikipedia by
  `occurredAt`.
- **No limit on line length.** A single enormous line would be buffered whole. Events are about
  1.3 KB. Not worth guarding against until a source misbehaves.
- **The 30 s connection timeout of the database pool** still blocks the reading thread when
  PostgreSQL is down (006). It then throws `HANDLER_FAILED`, and the consumer backs off. The
  server may drop us meanwhile, which the resume covers.
