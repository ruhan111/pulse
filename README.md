# Pulse

> What is changing on the internet right now, and why?

Pulse is a backend-focused event intelligence system. It continuously ingests activity from public
sources, normalizes it into a single event model, stores it, and detects unusual changes over time.
The user configures little and mostly observes; the system generates its own workload.

This is also a system-design learning project. It starts as a simple monolith and only becomes
distributed when measurements show a problem that distribution solves.

**Guiding principle:** never add infrastructure until we've identified the problem it solves.

## Current architecture

_Phase 3a: five real feeds are polled every 5 minutes and every event is stored once in PostgreSQL._

```
RSS feeds ──► RssPoller ──► fetch ─► parse ─► map ──► EventSink ──► JdbcEventSink ──► PostgreSQL
                                                                   (insert … on conflict do nothing)
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
| 3     | Persistence: PostgreSQL, migrations, idempotent ingestion, metrics     | in progress (3a done: PostgreSQL sink) |
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

### Looking at the data

Open a SQL prompt inside the container (no local install needed):

```bash
docker compose exec postgres psql -U pulse -d pulse
```

`events` rows are wide (summaries, attribute lists), so the default table output wraps across the
screen. This variant shows small results as a table and wide ones as one field per line:

```bash
docker compose exec postgres psql -U pulse -d pulse -P expanded=auto -P format=wrapped
```

Useful queries (end each with `;`, or psql waits for more input and shows `pulse-#`):

```sql
\dt                                                                       -- list tables
select count(*) from events;                                              -- how many events
select channel, count(*) from events group by channel order by 2 desc;    -- events per feed
select source, occurred_at, title from events order by occurred_at desc limit 20;  -- latest
select id, filename, dateexecuted from databasechangelog;                 -- applied migrations
```

`\x` toggles one field per line, `q` leaves the pager (`--More--` / `(END)`), `\q` quits psql. A
single query also works without the prompt:
`docker compose exec postgres psql -U pulse -d pulse -c "select count(*) from events"`.

For browsing, any PostgreSQL client (DBeaver, pgAdmin, IntelliJ) connects with `localhost:5432`,
database, user and password `pulse`.

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
