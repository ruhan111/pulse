# 006: Storing events in PostgreSQL (Phase 3a)

**Date:** 2026-10-03 · **Phase:** 3a · **Commit/PR:** Phase 3a PR

## Context

After 2d, Pulse logged new events but stored nothing. A restart forgot every seen id and reported
the whole current feed contents as new again, and `LoggingEventSink` only remembered the last
10,000 ids anyway. 3a puts PostgreSQL (already running via `compose.yaml`) behind the `EventSink`
port.

## Decision

- **`JdbcEventSink`** in `infrastructure` replaces `LoggingEventSink`, which is deleted. It keeps the
  `new event …` log line, so running the app looks the same.
- **Idempotency comes from the database.** The `events` table's primary key is the `EventId`
  (a UUID derived from `(source, externalId)`), with no auto-increment id column. One statement,
  `insert … on conflict (id) do nothing`, stores and detects duplicates at once: 1 affected row
  means `NEW`, 0 means `DUPLICATE`. There is no check-then-insert window for two writers to race through.
- **The first version wins.** A repeated id never overwrites the row, so `ingested_at` stays the
  first time Pulse saw the item. Phase 5 needs that to tell a backfill from fresh activity.
- **Schema:** enums as `text` (new sources need no migration), timestamps as `timestamptz`,
  attributes as `jsonb`. There's no unique constraint on `(source, external_id)` because it would
  duplicate the primary key. There are no indexes beyond the primary key until a query needs one
  (Phase 5's `occurred_at`).
- **Schema changes are Liquibase XML changesets** in `src/main/resources/db/changelog/`: a master
  file that includes one file per change (`changes/001-create-events-table.xml`). They run at startup.
- **SQL lives in files**, one statement per file, named after what it does:
  `src/main/resources/sql/insert_event_if_absent.sql`. `SqlFile.load(...)` reads them when the
  class using them is constructed, so a misnamed file stops the app at startup. Test queries
  follow the same rule in `src/test/resources/sql/`.
- **Data access with `JdbcClient`** and plain SQL. Attributes are passed as two `text[]` arrays
  and turned into JSON by Postgres (`jsonb_object(keys, values)`). That needs no JSON library and
  no hand-written escaping.
- **Liquibase analytics are off** (`spring.liquibase.analytics-enabled: false`). See below.
- **Tests run against a real PostgreSQL** via Testcontainers, using `postgres:17-alpine`, the same
  image as `compose.yaml`. `TestDatabase` starts one container per test run, shared by every test
  that needs it. `./mvnw test` now needs Docker running.

New dependencies: `spring-boot-starter-jdbc` (`JdbcClient`, Hikari pool),
`spring-boot-starter-liquibase`, the PostgreSQL driver, and `testcontainers-postgresql` (test only).

## Alternatives considered

- **JPA/Hibernate.** `save()` on an entity with an assigned id does a SELECT and then an INSERT: two
  round trips with a race between them, and it hides the `on conflict` behaviour the sink depends on.
  It would also need either annotations on `PulseEvent` (rejected in 001) or a mirror entity class.
- **jOOQ.** A code generator and a large library for one table and one statement.
- **Flyway with SQL migrations.** This was the first plan. I switched to Liquibase with XML:
  its changesets are database-independent and structured, and the changelog table records who
  applied what. The cost is more verbose files and about 2 s of startup (below).
- **`schema.sql` or Hibernate `ddl-auto`.** No history and no way to evolve a table that already
  holds data.
- **Upsert (`on conflict do update`).** It would overwrite `ingested_at` and break the port's
  contract ("accepting twice has the same effect as accepting once"). Edited feed items stay an
  open question from 001. They might become `EDITED` events.
- **SQL as Java strings.** Shorter, but the queries are harder to read and review. Files also open
  directly in a SQL client.
- **H2 in Postgres mode for tests.** `on conflict`, `jsonb` and `timestamptz` behave differently or
  not at all, so we'd be testing a database we don't run. Embedded Postgres (zonky) downloads
  binaries; Docker is already installed.
- **Spring Boot's `@ServiceConnection` container beans.** Each Spring context would get its own
  container, which Spring stops when that context closes. The restart test needs two contexts on
  the same data, so a plain static container is simpler and starts once.
- **Keeping `LoggingEventSink` behind a switch.** Configuration nobody needs.
- **Batching inserts.** About 86 events per backlog round doesn't justify it (see measurements).

## What happened

- **The cloud sandbox can run Docker.** Starting `dockerd` worked, so the tests, Testcontainers and
  `compose.yaml` all ran against a real Postgres 17 here. The real feeds are still blocked, so the
  app was pointed at a local copy of `rss2.xml`.
- **`SpringApplicationBuilder.properties()` sets only defaults.** The first version of the restart
  test passed the test database URL that way, and `application.yaml` won: the app tried
  `localhost:5432` and failed. Passing the settings as command-line arguments fixed it.
- **Liquibase phones home.** The sandbox proxy reported 7 blocked connections to
  `config.liquibase.com:443`, one per application start (test runs included). Liquibase 4.30+
  sends anonymous usage analytics by default. After `analytics-enabled: false`, there were none on the next runs. A trend finder has no
  business contacting a vendor on every start.
- **A short database outage doesn't fail a poll; a long one does, cleanly.** Stopping Postgres
  while the app polled every 2 s showed:
  - **8.3 s outage:** the poll during it blocked for 10.2 s waiting for a connection (Hikari's
    `connectionTimeout` is 30 s), then succeeded. No failure, nothing lost.
  - **45 s outage:** Hikari found all 10 pooled connections closed. The poll failed after
    30,024 ms with `PUBLISH_FAILED (CannotGetJdbcConnectionException)`. The next poll blocked
    until Postgres was back (13.2 s), then reported `new=0 duplicates=4`. The 2c design (keep old
    cache validators, resend everything, let duplicates be ignored) worked as intended against a
    real database.
- **A restart remembers.** The restart check in `RssIngestionEndToEndTest` passes: a second run
  reports `new=0 duplicates=4` and logs no new events. The real app showed the same thing: its
  first poll after a restart was `new=0 duplicates=4`.
- **Proving the tests.**
  - *Removed `on conflict (id) do nothing`:* 4 of 7 `JdbcEventSinkTest` tests errored with a
    duplicate-key exception, and the end-to-end test failed.
  - *Turned the insert into an upsert* (`do update set title = …, ingested_at = …`): 4 tests
    failed with `expected: DUPLICATE but was: NEW`, including the first-version-wins test.
  - Restored both times, and green.
- **Postgres rounds sub-microsecond timestamps rather than truncating them.**
  `12:00:00.123456789` is stored as `12:00:00.123457`. There's a test for it. It matters only if
  something ever compares a stored timestamp with the in-memory `Instant`.

## Measurements

Insert latency, one thread, one statement per event, autocommit, Postgres 17 in Docker on the same
4-core sandbox, 5,000 events after a 1,000-event warm-up (throwaway test, not committed):

| Case      | mean     | p50      | p99      | max     | throughput |
|-----------|---------:|---------:|---------:|--------:|-----------:|
| new       | 0.387 ms | 0.361 ms | 0.764 ms | 10.0 ms | ~2,600/s   |
| duplicate | 0.279 ms | 0.259 ms | 0.546 ms | 8.3 ms  | ~3,600/s   |

- **The backlog round from 005 (86 events) would cost about 33 ms of inserts.** That's noise next to
  the fetch times (up to 3.9 s). Batching isn't justified until a source produces thousands of
  events per second, which may happen in Phase 4 with Wikipedia.
- **Startup went from 0.9 s (2d) to 3.5–3.9 s.** Liquibase takes about 2.0 s of it: its first log
  line comes at +0.9 s and "update successful" at +2.9 s, on both the first run (creating the
  table) and later runs (nothing to apply).
- **The test suite now has 63 tests** (7 new sink tests; the logging sink's test was removed). The
  first database test pays about 9 s for the container to start, and later ones reuse it.

## Consequences / open questions

- Events survive restarts, and "no duplicates on re-poll" (the correctness SLO) is now enforced by a
  primary key, not by memory.
- Running the app needs `docker compose up -d` first. Without a database the app refuses to start
  (exit 1 after 3.6 s), which is intended.
- `./mvnw test` needs Docker. Without it, the database tests fail with a Testcontainers error.
- **A long outage blocks polling for 30 s per attempt.** That's fine with one sequential poller.
  With several sources, one slow database would stall all of them. That's an argument for
  decoupling ingestion from storage, but only once there is evidence.
- **Hikari keeps 10 connections for a single writer thread.** Harmless for now; size the pool when
  there's more than one writer.
- Next: 3b, metrics (Micrometer/Actuator): events new and duplicate per source, poll outcomes,
  insert latency. Then 3c, the long real-feed run, measured with those metrics.
