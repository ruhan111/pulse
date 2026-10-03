# Pulse

> What is changing on the internet right now, and why?

Pulse is a backend-focused event intelligence system. It continuously ingests activity from public
sources, normalizes it into a single event model, stores it, and detects unusual changes over time.
The user configures little and mostly observes; the system generates its own workload.

This is also a system-design learning project. It starts as a simple monolith and only becomes
distributed when measurements show a problem that distribution solves.

**Guiding principle:** never add infrastructure until we've identified the problem it solves.

## Current architecture

_Phase 3b: five real feeds are polled every 5 minutes, every event is stored once in PostgreSQL,
and what happens is measured._

```
RSS feeds ──► RssPoller ──► fetch ─► parse ─► map ──► EventSink ──► JdbcEventSink ──► PostgreSQL
                 │                                                  (insert … on conflict do nothing)
                 └── RssMetrics                                         └── sink metrics
                         └──────────────► Micrometer ◄──────────────────┘ ──► /actuator/metrics
```

The `events` table's primary key is the `EventId`, so re-polling a feed or restarting the app never
stores an item twice. Schema changes are Liquibase XML changesets in
`src/main/resources/db/changelog/`. SQL lives in `src/main/resources/sql/`, one statement per file,
named after what it does.

Target for the first working slice:

```
RSS feeds ──► RssAdapter ──► PulseEvent ──► EventSink ──► PostgreSQL
                                                              │
                                              trend detection ◄┘ ──► REST API
```

## Domain language

| Concept      | Meaning                                                                        |
|--------------|--------------------------------------------------------------------------------|
| `PulseEvent` | Something that happened at a source (a post, an edit, a release), normalized   |
| `Source`     | Which kind of source produced an event (RSS, Hacker News, GitHub, …)           |
| `EventType`  | What happened, independent of where: published, edited, commented              |
| `EventId`    | Identity derived from `(source, externalId)`, so the same item always has the same id |
| `Topic`      | The thing that trends: a normalized key such as a term or a domain             |
| `Mention`    | One occurrence of a topic in one event: the unit that gets counted             |
| `Trend`      | A topic whose activity is unusually high compared with its baseline            |
| `EventSink`  | Port the ingestion side publishes into, so the rest of the system can change without touching sources |

## Package structure

Packages follow business responsibilities, not technical layers. Dependencies point inward,
toward `event`:

```
com.example.pulse
├── event/            domain core: PulseEvent, EventId, Source, EventType, EventSink (port)
├── ingestion/        source adapters (rss/, hackernews/, …) → publish into EventSink
├── trend/            Topic, Mention, MentionExtractor; later counting, baselines, detection
├── api/              HTTP interface
└── infrastructure/   implementations of ports with real technology (PostgreSQL, later Kafka…)

          api ─────────┐
                       ▼
 ingestion ──► event ◄── trend
                 ▲
 infrastructure ─┘  (may use everything; nothing uses it)
```

| Package          | May depend on                  | Must not depend on                     |
|------------------|--------------------------------|----------------------------------------|
| `event`          | the JDK only                   | Spring, any other Pulse package        |
| `ingestion`      | `event`                        | `trend`, `api`, `infrastructure`; nothing outside depends on it |
| `trend`          | `event`                        | `ingestion`, `api`, `infrastructure`   |
| `api`            | `event`, `trend`               | `ingestion`, `infrastructure`          |
| `infrastructure` | everything                     | nothing depends on it                  |

These rules are enforced by [`ArchitectureTest`](src/test/java/com/example/pulse/ArchitectureTest.java),
so breaking one fails the build.

## Roadmap

| Phase | Goal                                                                   | Status  |
|-------|------------------------------------------------------------------------|---------|
| 1     | Domain: `PulseEvent`, `Source`, `EventType`, `Topic`, `Mention`        | done    |
| 2     | Ingestion: one RSS adapter producing real events                       | built (2a–2d); real-feed run pending |
| 3     | Persistence: PostgreSQL, migrations, idempotent ingestion, metrics     | in progress (3a PostgreSQL sink, 3b metrics done; 3c long run next) |
| 4     | Multiple sources: more feeds plus a high-volume source (Wikipedia / HN) | planned |
| 5     | Trend detection: per-mention counts, baselines, spike detection       | planned |
| 6     | API: expose trends and the events behind them                          | planned |
| 7     | Load testing: synthetic generator, measure against SLOs                | planned |
| 8     | Scale based on evidence (Kafka, Redis, ClickHouse… only if justified)  | planned |

## Service level objectives (draft)

These give the load tests something to be measured against:

- **Freshness:** a spike is visible via the API within 5 minutes of the underlying events occurring.
- **Correctness:** no lost events; re-polling a source never creates duplicates.
- **Isolation:** ingestion keeps working while trend processing is slow or down.

## Journal

The project's history is documented step by step in [`docs/journal/`](docs/journal/). Each entry
records what was built, which decisions were made and why, what broke, and what was measured.

## Running locally

Requires Java 21 and Docker.

```bash
docker compose up -d     # start PostgreSQL first; the app refuses to start without it
./mvnw spring-boot:run   # applies migrations, polls the feeds in application.yaml, stores new events
./mvnw test              # needs Docker running (Testcontainers); tests never touch the internet
```

## Metrics and health

While the app runs, two endpoints are available on `localhost:8080` (not reachable from the network):

```bash
curl localhost:8080/actuator/health                    # UP/DOWN, including the database
curl localhost:8080/actuator/metrics                   # names of all metrics
curl "localhost:8080/actuator/metrics/pulse.events.accepted?tag=result:NEW"   # new events so far
```

| Metric | What it answers |
|---|---|
| `pulse.events.accepted` (`source`, `result`) | How many events were new vs. already stored |
| `pulse.rss.polls` (`feed`, `outcome`) | Poll count and duration per feed, by outcome |
| `pulse.rss.fetch.failures` (`feed`, `reason`) | Why fetches fail, per feed |
| `pulse.rss.entries.skipped` (`feed`, `reason`) | Feed entries that couldn't become events |
| `pulse.sink.insert` (`result`) | Insert latency (p50/p99 via `pulse.sink.insert.percentile`) |
| `pulse.events.lateness` (`source`) | How old new events are when first stored (freshness) |
| `pulse.events.future` (`source`) | New events a source dated in the future |
| `hikaricp.connections.*` | Database connection pool usage |

Counters reset when the app restarts; there is no history yet.

## Local database

PostgreSQL runs in Docker (Docker Desktop on Windows/macOS). The app creates and updates its tables
itself at startup (Liquibase).

```bash
docker compose up -d     # start Postgres (localhost:5432, database/user/password: pulse)
docker compose ps        # STATUS should say "Up"
docker compose stop      # stop it; data is kept
docker compose down      # remove the container; data is still kept (it lives in a volume)
docker compose down -v   # ⚠ also deletes the volume, i.e. all data
```

Connect with any SQL client, or from the container itself:

```bash
docker compose exec postgres psql -U pulse -d pulse
```

```sql
select source, occurred_at, title from events order by occurred_at desc limit 20;
```

### Backup and restore

The data survives restarts and container removal, but not `down -v`, a Docker Desktop reset, or a
major Postgres upgrade. A backup is a plain SQL file. These commands avoid shell redirection
(`>`), which writes UTF-16 files in Windows PowerShell 5:

```bash
# backup to backups/pulse.sql (ignored by git)
docker compose exec postgres pg_dump -U pulse -d pulse -f /tmp/pulse.sql
docker compose cp postgres:/tmp/pulse.sql backups/pulse.sql

# restore into an empty database
docker compose cp backups/pulse.sql postgres:/tmp/pulse.sql
docker compose exec postgres psql -U pulse -d pulse -f /tmp/pulse.sql
```
