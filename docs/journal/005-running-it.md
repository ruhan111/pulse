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
- **An end-to-end test** (`RssIngestionEndToEndTest`) starts the whole application against a local
  feed server, polls every 100ms, and checks that the 4 valid items in `rss2.xml` are logged
  exactly once across repeated polls.

## Alternatives considered

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
  with 24 lines instead of 4: 6 poll rounds × 4 items. Restored, and green.

## Measurements

Pending: a run against the real feeds, from a machine that can reach them. What to record:

- per feed: items per poll, skipped entries by reason, whether it supports `ETag` / `Last-Modified`
  (how many polls end in `NOT_MODIFIED`), and poll duration;
- how many *new* events per hour across all feeds, which is the real input rate Phase 3 must handle;
- anything the sample files didn't predict.

These go in the next journal entry, since entries aren't rewritten after merging.

## Consequences / open questions

- The app is now useful on its own: `./mvnw spring-boot:run` prints new items from five sources
  as they appear.
- Nothing is stored. A restart forgets every seen id and logs the current feed contents again.
  That is the problem Phase 3 (PostgreSQL) solves.
- Still open from 004: sequential polling, and no backoff for feeds that keep failing. The
  real-feed run will show whether either matters.
