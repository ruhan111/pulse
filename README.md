# Pulse

> What is changing on the internet right now, and why?

Pulse is a backend-focused event intelligence system. It continuously ingests activity from public
sources, normalizes it into a single event model, stores it, and detects unusual changes over time.
The user configures little and mostly observes; the system generates its own workload.

This is also a system-design learning project. It starts as a simple monolith and only becomes
distributed when measurements show a problem that distribution solves.

**Guiding principle:** never add infrastructure until we've identified the problem it solves.

## Current architecture

_Phase 1: the domain model exists and the package rules are enforced by tests. Nothing ingests data yet._

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
| `ingestion`      | `event`                        | `trend`, `api`, `infrastructure`       |
| `trend`          | `event`                        | `ingestion`, `api`, `infrastructure`   |
| `api`            | `event`, `trend`               | `ingestion`, `infrastructure`          |
| `infrastructure` | everything                     | nothing depends on it                  |

These rules are enforced by [`ArchitectureTest`](src/test/java/com/example/pulse/ArchitectureTest.java),
so breaking one fails the build.

## Roadmap

| Phase | Goal                                                                   | Status  |
|-------|------------------------------------------------------------------------|---------|
| 1     | Domain: `PulseEvent`, `Source`, `EventType`, `Topic`, `Mention`        | done    |
| 2     | Ingestion: one RSS adapter producing real events                       | planned |
| 3     | Persistence: PostgreSQL, migrations, idempotent ingestion, metrics     | planned |
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

Requires Java 21. There is no pipeline to run yet, but the tests run:

```bash
./mvnw test
```
