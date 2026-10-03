# 005: A sink, real feeds, and running it (Phase 2d)

**Date:** 2026-10-03 · **Phase:** 2d · **Commit/PR:** Phase 2d PR

## Context

After 2c, everything existed except something to publish into. 2d closes the loop: an `EventSink`
implementation, real feeds in `application.yaml`, and the app running end to end.

## Decision

- **`LoggingEventSink`** in `infrastructure`: logs each new event once. It honors the sink's
  idempotency contract by remembering the last 10,000 `EventId`s, so re-polled items aren't logged
  again. Memory is bounded, at the cost that a very old item could be logged twice. Fine for a
  temporary sink; Phase 3's database gives the real guarantee.
- **RSS is on in `application.yaml`**, with five feeds chosen for variety: Hacker News front page
  (hnrss.org), Lobsters, Ars Technica, BBC Technology and arXiv cs.AI. Tech news, community links
  and research, from different publishers and feed generators.
- **`HttpFeedFetcher` honors the JVM proxy settings.** The JDK `HttpClient` ignores
  `-Dhttps.proxyHost` unless given `ProxySelector.getDefault()`. Without it, Pulse couldn't run behind
  a corporate proxy. With no proxy configured it connects directly, as before.
- **Tests never touch the internet.** Any test that starts the full app either disables RSS or
  points it at a local server.
- **`EventSink.accept` returns `Accepted.NEW` or `Accepted.DUPLICATE`** (added after the first
  real run, see below). The poll log now says `new=0 duplicates=25` instead of `published=25`, so
  you can see at a glance whether a feed brought anything new. This changes the domain port; the
  sink is the only component that knows whether an event is new, so it has to report it.
- **Console logs are UTF-8** (`logging.charset.console`), after em dashes and curly quotes in
  titles showed up as `�` on a Windows console.
- **An end-to-end test** (`RssIngestionEndToEndTest`) starts the whole application against a local
  feed server, polls every 100ms, and checks that the 4 valid items in `rss2.xml` are logged
  exactly once across repeated polls.

## Alternatives considered

- **Reporting new vs. duplicate without changing the port**: the sink logging its own periodic
  summary (counts no longer line up with feeds or rounds), or the poller remembering ids itself
  (duplicates the sink's job and disagrees with the database after a restart). A future Kafka sink
  can't know whether an event is a duplicate; it will need a third value like `ACCEPTED`, added
  when Kafka actually arrives.
- **A sink that logs every `accept`.** Simpler, but it breaks the idempotency contract, and the log
  would fill with the same items every 5 minutes.
- **An unbounded set of seen ids.** Memory grows forever in a long-running process.
- **A `src/test/resources/application.yaml` that disables RSS.** It would *replace* the main
  `application.yaml` on the test classpath, so the configuration tests would no longer check the
  shipped file. Disabling per test is explicit and keeps them honest.

## What happened

- **The cloud sandbox can't reach the feeds.** Its network policy blocks all five hosts, so I
  couldn't run against real data here. Running the app with the shipped configuration still showed
  the whole path working: it started in 0.9 s, polled all five feeds, and each poll ended in a
  clean `FETCH_FAILED (NETWORK_ERROR: Tunnel failed, got: 403)` in 200–300 ms, which is the
  sandbox's proxy refusing the connection, with no crash and the next feed still polled.
- **The proxy fix came out of that run.** Before it, the client would have ignored the proxy
  settings entirely and tried to connect directly.
- **Proving the end-to-end test.** I temporarily made the sink log every `accept`. The test failed
  with 24 lines instead of 4: 6 poll rounds × 4 items. Later, making the sink report every event as
  `NEW` also failed it (no `new=0 duplicates=4` line). Restored, and green both times.
- **The first real run** (on my machine, 3 rounds at a 30 s interval) worked end to end, and the
  log raised a question it couldn't answer: Lobsters said `published=25` every round, but was
  anything new? Only the sink knew. That led to `Accepted`.

## Measurements

First real run, Saturday 2026-10-03 19:24, poll interval 30 s, 3 rounds:

| Feed           | Items | Conditional GET          | First poll | Later polls |
|----------------|------:|--------------------------|-----------:|------------:|
| Hacker News    | 20    | yes (304)                | 3914 ms    | 330 ms      |
| Lobsters       | 25    | no, full feed every time | 585 ms     | 550 ms      |
| Ars Technica   | 20    | no                       | 81 ms      | 40–50 ms    |
| BBC Technology | 21    | no                       | 114 ms     | 27–64 ms    |
| arXiv cs.AI    | 0     | yes (304)                | 29 ms      | 12–17 ms    |

- **86 events on the first round, 0 new in the next minute.** Real RSS volume is tiny once the
  backlog is in. A longer run is still needed for a real new-events-per-hour number.
- **Only 2 of 5 feeds support conditional GET.** The other three resend about 66 items every round;
  the idempotent sink absorbs them.
- **The first poll is slow** (3.9 s for Hacker News): DNS, TLS handshake and JIT warm-up. Only the
  first request pays it.
- **arXiv was empty**: it doesn't publish announcements at weekends. An empty feed isn't
  necessarily a broken feed.
- **`skipped=0` everywhere.** Real feeds were cleaner than my samples. The 2a edge cases didn't
  occur here, but they cost nothing to keep.
- **The first poll is a backlog**: BBC items went back to Sept 8. If trends were counted by
  `ingestedAt`, every restart would look like a spike. Phase 5 must count by `occurredAt` and treat
  the first poll as a backfill.
- **The same story appeared in several sources**: Apple's Full Disk Access change on Hacker News,
  Lobsters *and* Ars; Kolibri twice on Hacker News from two different sites. This is exactly the
  cross-source signal Phase 5 should detect, already visible in 86 events.

## Consequences / open questions

- The app is now useful on its own: `./mvnw spring-boot:run` prints new items from five sources
  as they appear.
- Nothing is stored. A restart forgets every seen id and logs the current feed contents again.
  That is the problem Phase 3 (PostgreSQL) solves.
- Still open from 004: sequential polling, and no backoff for feeds that keep failing. The
  real-feed run will show whether either matters.
