# 004: Polling feeds on a schedule (Phase 2c)

**Date:** 2026-10-03 · **Phase:** 2c · **Commit/PR:** Phase 2c PR

## Context

2a maps entries, 2b fetches documents. 2c connects them: for every configured feed, on a
schedule, fetch → parse → map → publish into `EventSink`. This is the first orchestration code, so
the interesting questions are about failure: what breaks what, and what is lost when it does.

## Decision

- **`RssPoller`** runs one round over all feeds. `poll(feed)` returns a `PollResult` with an
  `Outcome` (`PUBLISHED`, `NOT_MODIFIED`, `FETCH_FAILED`, `PARSE_FAILED`, `PUBLISH_FAILED`), counts
  of published and skipped entries by reason, and the duration. Every poll logs one line from it.
- **Failure isolation at three levels.** A bad entry becomes a `Skipped` result (including a new
  `INVALID_ENTRY` backstop for anything that slips past the mapper's checks). A handled failure
  (timeout, 503, not a feed) becomes a `PollResult`. An unexpected exception in one feed is caught
  and logged, and the next feed is still polled.
- **Validators are stored only after everything is published.** If the sink fails halfway, the
  `ETag` is *not* saved, so the next poll fetches the whole feed again instead of getting a 304 and
  silently losing the unpublished events. The events that did get through are published a second
  time, which is fine because sinks must be idempotent on `EventId`. This is at-least-once delivery:
  the same idea as committing a Kafka offset only after processing.
- **Per-feed state is in memory.** Losing it on restart costs one full fetch per feed, which is
  cheap and harmless thanks to idempotency. Not worth a database yet.
- **Configuration lives in `application.yaml`** under `pulse.rss`: `enabled` (false), `feeds`,
  `poll-interval` (5m), `fetch-deadline` (10s), `max-feed-size` (5MB). `RssProperties` is the typed
  contract that reads it: it converts `5m` to a `Duration` and rejects missing settings at startup
  ("pulse.rss.poll-interval must be set") instead of failing on the first poll. The configuration
  tests load the real `application.yaml`, so they check the shipped values.
- **`RssConfiguration`** wires everything and schedules `pollAll` with a *fixed delay*: the next
  round starts a fixed time after the previous one finished, so slow rounds can't overlap.
- **One shared `Clock` bean** in `infrastructure.ClockConfiguration`, keeping `PulseApplication` a pure entry point. Components ask for `java.time.Clock` by type, so this doesn't break "nothing depends on infrastructure".
- **Encapsulation.** Everything in `ingestion.rss` is now package-private except `RssConfiguration`
  and `RssProperties`. `FeedFetcher` became an interface with `HttpFeedFetcher` as the HTTP
  implementation, so the poller can be tested with a fake. A new `ArchitectureTest` rule says nothing
  outside `ingestion` may depend on it: sources are wired by Spring and only talk to `EventSink`.

## Alternatives considered

- **Splitting `ingestion.rss` into subpackages.** Rejected for now: Java can only hide classes
  within one package, so subpackages would force internals to be public. Revisit in Phase 4 when a
  second HTTP source makes `ingestion.http` a real, shared responsibility.
- **`@Scheduled(fixedDelayString = "${…}")`.** Rejected in favour of registering the task with the
  typed `Duration` from `RssProperties`: one source of truth for the interval, validated at startup.
- **Fixed rate.** Rejected: with slow feeds, rounds could start while the previous one is running.
- **Storing validators straight after the fetch.** Rejected, and tested (see below).
- **Polling feeds in parallel.** Not needed at a handful of feeds. One slow feed delays the round
  by at most the fetch deadline. A candidate for load testing in Phase 7.
- **Defaults in code (`@DefaultValue` on the record).** My first version. Rejected: values would
  live in two places that can drift apart, and you'd have to read Java to see what's configurable.
- **Making `pulse.rss.enabled` default to true.** Rejected: the app would then refuse to start until
  an `EventSink` exists. Off by default also lets later load tests run without real polling.

## What happened

- **A bug I caught before running anything.** The configuration used `proxyBeanMethods = false`
  and the scheduling callback called `rssPoller()` directly. Without proxying, that creates a
  *second* poller, so the scheduled one and the Spring bean would have had separate state.
  Fixed by injecting the bean into the scheduling configuration.
- **A test made a real network call.** Scheduling starts polling immediately, so the configuration
  test fired a request at `news.example.com` before the context closed. That's the right behaviour
  for the app, but tests shouldn't touch the internet. The test feed now points at a closed port on
  localhost.
- **Proving the validator rule matters.** I temporarily moved `validators.put` to before
  publishing. `keepsOldValidatorsWhenPublishingFails` failed: the second fetch sent `"v1"`, which
  would have produced a 304 and lost the events the sink never received. Restored, and green.

## Measurements

None from real feeds yet. 54 tests pass (7 poller, 4 configuration, 1 new architecture rule).

## Consequences / open questions

- Feeds are polled one after another, so a round takes up to `feeds × fetch-deadline` in the worst
  case. Fine for now; measure in 2d.
- One feed failing permanently is retried every round, with no backoff. Add it if a real feed
  misbehaves in 2d.
- There's still no `EventSink` implementation, so `pulse.rss.enabled=true` won't start yet.
- Next: 2d, a logging sink in `infrastructure`, real feeds in `application.yaml`, and running it.
