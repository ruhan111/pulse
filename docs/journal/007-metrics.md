# 007: Metrics (Phase 3b)

**Date:** 2026-10-03 · **Phase:** 3b · **Commit/PR:** Phase 3b PR

## Context

After 3a, events are stored, but the only way to see what Pulse is doing is to read log lines.
Questions like "how many new events per hour?", "how often does this feed fail?" or "how slow are
inserts?" mean grepping by hand. Journals 004–006 left questions that need numbers: parallel
polling, backoff for failing feeds, pool size, batching, and the freshness SLO. Journal 000 said
metrics start in Phase 3, because decisions need evidence.

## Decision

- **Micrometer through Spring Boot Actuator**, read over HTTP at `/actuator/metrics` and
  `/actuator/health`. No Prometheus or Grafana yet (see alternatives).
- **A web server now** (`spring-boot-starter-webmvc`, embedded Tomcat), because Actuator's endpoints
  need HTTP. Phase 6's API needs it anyway, so it arrives early rather than being extra. It's bound to
  `127.0.0.1:8080`, and only `health` and `metrics` are exposed.
- **What is measured, and where:**

  | Metric | Type | Tags | Recorded in |
  |---|---|---|---|
  | `pulse.rss.polls` | timer | `feed`, `outcome` | `RssMetrics`, from every `PollResult` |
  | `pulse.rss.entries.skipped` | counter | `feed`, `reason` (`SkipReason`) | `RssMetrics` |
  | `pulse.rss.fetch.failures` | counter | `feed`, `reason` (`FetchFailure`) | `RssMetrics` |
  | `pulse.events.accepted` | counter | `source`, `result` (`NEW`, `DUPLICATE`) | `JdbcEventSink` |
  | `pulse.sink.insert` | timer, p50/p99 | `result` (`NEW`, `DUPLICATE`, `FAILED`) | `JdbcEventSink` |
  | `pulse.events.lateness` | timer, p50/p99 | `source` | `JdbcEventSink`, new events only |
  | `pulse.events.future` | counter | `source` | `JdbcEventSink`, new events dated in the future |

  Hikari pool metrics (`hikaricp.connections.*`), JVM metrics and a database health check come
  with Actuator for free.
- **The earlier result types pay off.** `Outcome`, `SkipReason` and `FetchFailure` were made enums
  in 2a–2c "so they can be counted". Now they are tag values directly, with no mapping code.
- **Tags have a bounded number of values.** `feed` comes from configuration (5 URLs). Event ids
  and titles are never tags: each distinct value creates a new time series, so memory would grow
  forever.
- **The sink records new vs. duplicate**, because it's the only component that knows. The poller
  records what only it knows: feed, outcome, skips and fetch failures.
- **Lateness counts only new events.** A duplicate's age says nothing about freshness. Micrometer
  timers silently drop negative durations, so events dated in the future (a source's clock is
  ahead) get their own counter instead of disappearing.
- **Percentile and max windows are 1 hour, not Micrometer's default 2 minutes.** With polls every
  5 minutes, a 2-minute window would show nothing most of the time.
- **Failed inserts are timed too** (`result=FAILED`), so a database outage is visible in the
  metrics and not just in the log.

## Alternatives considered

- **Prometheus + Grafana in `compose.yaml`.** Real history and graphs, and the industry standard.
  Deferred: nothing has needed history yet. 3c (the long real-feed run) will show whether current
  totals are enough. If they aren't, adding Prometheus will be the measured need.
- **Micrometer's logging registry** (every metric written to the log each minute). It needs no new
  infrastructure, but you would be grepping again.
- **JMX instead of a web server.** No new dependency, but clumsy to read, and Phase 6 needs HTTP
  anyway.
- **Hand-written counters.** That would mean reinventing timers, percentiles and tags.
- **Recording metrics in `pollAll()`.** Tests call `poll()` directly, so they would have seen no
  metrics. Recording inside `poll()` covers both paths.
- **Recording new/duplicate in the poller.** It only knows what the sink returns; putting the
  counter in the sink also covers future sources that don't go through `RssPoller`.

## What happened

- **The sandbox's Docker daemon had stopped since 3a**, and every database test failed with "Could
  not find a valid Docker environment". Restarting `dockerd` fixed it. It's a reminder that
  `./mvnw test` now depends on Docker running.
- **The real app against the compose database**, with one local feed that always returns the full
  `rss2.xml` and one closed port, polling every 2 s:
  - All 7 `pulse.*` metrics plus `hikaricp.connections.*` appeared in `/actuator/metrics`.
  - After 9 rounds: `pulse.events.accepted{result=DUPLICATE}` = 36, `pulse.rss.polls` = 9
    `PUBLISHED` and 9 `FETCH_FAILED`, `pulse.rss.fetch.failures{reason=NETWORK_ERROR}` = 9, and 36
    skipped entries (4 per round). Every number matched the log.
  - `/actuator/health` showed `db: UP`. `/actuator/env` returned 404, and a request to the
    machine's network address was refused, so it really is localhost-only.
- **Lateness exposed undated items.** After emptying the table, the 4 events came back as new with
  lateness values of about 0 s, about 0 s, and about 3.5 days. The two "fresh" ones are the items in
  `rss2.xml` without a date: 2a's mapper uses the ingestion time as `occurredAt` for them, so they
  look perfectly fresh. A metric written for the freshness SLO immediately showed that undated
  items distort it. Phase 5 has to decide what an undated item means.
- **The health endpoint takes 30 s to report a database outage.** With Postgres stopped,
  `/actuator/health` answered `503 DOWN` only after 30,045 ms, both times. The health check waits for
  a connection like everything else, and Hikari's `connectionTimeout` is 30 s. Meanwhile
  `pulse.sink.insert{result=FAILED}` counted 2 inserts of 30.0 s each, and
  `hikaricp.connections.timeout` counted 4. After Postgres came back, health returned to `UP`.
- **Proving the tests.**
  - *Removed the fetch-failure metric:* `countsFetchFailuresByReason` failed.
  - *Recorded lateness for duplicates too:* the lateness test failed with `expected: 1L but was: 2L`.
  - *Tagged new events `new` instead of `NEW`:* the end-to-end test's HTTP check failed with a
    404 from `/actuator/metrics/pulse.events.accepted?tag=result:NEW`.
  - Restored all three, and green.

## Measurements

- **Startup: 5.8 s, up from 3.5–3.9 s in 3a** (one run, same sandbox). Tomcat and Actuator add
  about 2 s. That's acceptable for a long-running process, but worth noting: startup has gone
  0.9 → 3.7 → 5.8 s across 2d, 3a and 3b.
- **Insert latency from the metrics** (36 duplicate inserts in the running app): mean 3.5 ms,
  max 65 ms. The max is the first insert after startup (JIT and pool warm-up). The 3a benchmark's
  0.28 ms mean was taken after 1,000 warm-up inserts, so the two numbers measure different things.
- **Hikari:** 38 connection acquisitions, max wait 2.5 ms, 0 active between polls. The pool of 10 is
  far larger than one writer needs, which matches the open question from 006.
- **Tests: 70**, up from 63 (3 poller metric tests, 4 sink metric tests). The end-to-end test now
  also checks the endpoints over HTTP.

## Consequences / open questions

- Questions like "is anything new arriving?" and "is a feed failing?" are now answered with one
  HTTP request instead of grep.
- The numbers reset on every restart and have no history. 3c's long run will show whether that's
  enough, or whether Prometheus is justified.
- **Health takes 30 s to say DOWN.** Lowering `spring.datasource.hikari.connection-timeout`
  (e.g. 5 s) would make health and failed polls fast. But then an 8 s outage, which 006 showed
  being absorbed without any failure, would turn into a `PUBLISH_FAILED`. Leave it until something
  actually polls the health endpoint.
- **Undated items look perfectly fresh** (lateness 0). Phase 5 must decide how to treat them before
  they count toward trends.
- Next: 3c, the long real-feed run on my machine, read through these metrics: new events per hour
  per feed, failure rates, 304 share, lateness, and pool usage.
