# Pulse

A trend finder built as a system-design learning project: it continuously ingests public sources
(RSS first, later Hacker News, GitHub, Wikipedia…), normalizes everything into `PulseEvent`s and
detects unusual activity. Spring Boot 4, Java 21, Maven.

**Guiding principle:** never add infrastructure until a measured problem justifies it. The system
starts as a modular monolith; Kafka, Redis etc. only arrive with evidence (see the README roadmap).

## Where things are

- `README.md`: what Pulse is *now*. Architecture, domain language, package rules, roadmap with status.
- `docs/journal/`: how it got here. Append-only entries, one per phase part. Read the latest
  entries to learn current status and open questions before starting work.

## Commands

```bash
docker compose up -d   # PostgreSQL; the app needs it, tests start their own via Testcontainers
./mvnw test        # all tests, needs Docker running; if permission denied: sh mvnw test
./mvnw spring-boot:run
```

## Architecture

Packages are organized by business responsibility, never by technical type (no `service/`,
`controller/`, `mappers/`, `exceptions/`). Dependencies point inward toward `event`:

| Package          | May depend on      | Notes                                         |
|------------------|--------------------|-----------------------------------------------|
| `event`          | the JDK only       | Domain core. No Spring, no libraries.         |
| `ingestion`      | `event`            | One subpackage per source, e.g. `ingestion.rss`. Nothing outside depends on it; Spring wires it in. |
| `trend`          | `event`            |                                               |
| `api`            | `event`, `trend`   |                                               |
| `infrastructure` | everything         | Nothing may depend on it.                     |

These rules are enforced by `ArchitectureTest` (ArchUnit). Change a rule only deliberately, and
record why in the journal.

- Ingestion publishes into the `EventSink` port and never knows what's behind it.
- Streaming sources keep their resume position in the `CheckpointStore` port (opaque per-stream
  string, saved only once everything before it was published).
- Facts learned about an event later (e.g. "this edit was reverted") go into the `EventAnnotations`
  port, keyed by `EventId`. Events are never rewritten; annotations may arrive before their event.
- `EventId` is derived from `(source, externalId)`, so the same item always gets the same id.
  `externalId` must be stable across polls.
- Schema changes are Liquibase XML changesets: one file per change in
  `src/main/resources/db/changelog/changes/`, included from `db.changelog-master.xml`. Never edit an
  applied changeset; add a new one.
- SQL lives in `src/main/resources/sql/` (tests: `src/test/resources/sql/`), one statement per file,
  named after what it does (`insert_event_if_absent.sql`), loaded with `SqlFile.load(...)`.
- Prefer a flat package with package-private internals over subpackages. Split only when real
  duplication appears (e.g. extract `ingestion.http` when a second HTTP source needs the fetcher).

## Code style

- Tabs for indentation (match the existing code).
- Records for value types, validated in a compact constructor. Check fields in the order they are
  declared. Normalize where sensible (strip text, default optional text to `""`, copy collections).
- `import static java.util.Objects.requireNonNull;` and call `requireNonNull(...)`, never
  `Objects.requireNonNull(...)`. Static imports go in their own group after regular imports.
- No `package-info.java` files.
- Expected failures are results, not exceptions: sealed interfaces such as `MappedEntry`
  (`Mapped` / `Skipped`) and `FetchResult` (`Fetched` / `NotModified` / `Failed`), with enum
  reasons so they can be counted as metrics.
- Metrics use Micrometer with enum names as tag values. Tags must have a bounded set of values
  (feed URLs from configuration are fine; event ids, titles or URLs from feeds never are).
- Inject `Clock` instead of calling `Instant.now()`.
- Javadoc explains *why*, not what. Keep comments short.
- Prefer the JDK over new dependencies; justify every dependency added.

## Testing

- JUnit 5 + AssertJ. Sample data in `src/test/resources/`.
- HTTP tests use the JDK's built-in `com.sun.net.httpserver.HttpServer` on localhost.
- When a test protects against an important failure, prove it can fail: break the code
  temporarily, see the test go red, restore it. Note the result in the journal.
- Check library behaviour directly when the samples might not reveal it.

## Workflow

- Work is split into phases, and phases into small parts (e.g. 2a, 2b, 2c). Before coding, explain
  the plan and the design decisions; wait for the go-ahead.
- Each part ends with: passing tests, a journal entry, the README roadmap status updated, then a PR.
- Every piece of work gets its own branch, created from `origin/main` and named after what is being
  worked on, with `_` instead of spaces: e.g. `phase_2d_logging_sink`, `move_clock_config`. This
  applies even when a session suggests another branch name.
- One PR per branch. The user reviews and merges; the next piece of work starts a new branch from
  the updated `origin/main`.
- When explaining, cover why a component exists, what problem it solves, what happens when it
  fails, and the alternatives that were rejected.

## Journal entries

File `docs/journal/NNN-short-title.md`, added to the table in `docs/journal/README.md`. Sections:
Context, Decision, Alternatives considered, What happened, Measurements, Consequences / open
questions. Never rewrite old entries; if one turns out wrong, say so in a new one. Prefer numbers
over adjectives.
