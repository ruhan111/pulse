# 001: The domain model and package boundaries

**Date:** 2026-09-30 · **Phase:** 1 · **Commit/PR:** Phase 1 PR

## Context

Before fetching any real data, Pulse needs a shared language: what an event is, how two events
are recognized as the same one, and what "a trend" is actually counting. The package structure also
needs to encode the architecture, so later phases can't quietly erode it.

## Decision

**`event` is the domain core and depends on nothing but the JDK.**

- `PulseEvent` is an immutable record, validated in its constructor. If an instance exists, it's
  well-formed; downstream code never re-checks. Text is stripped, `summary` defaults to empty, and
  `attributes` is defensively copied.
- `EventId` is *derived* from `(source, externalId)` with a name-based UUID, not generated randomly
  and not stored separately. Re-polling a feed, retrying a request or replaying a stream always
  gives the same id. Idempotency starts in the domain; the database only has to enforce uniqueness.
- `occurredAt` and `ingestedAt` are both kept. `lateness()` is their difference, which is the raw
  material for freshness metrics and for handling out-of-order events. It may be negative (source
  clocks lie); what to do about that is ingestion policy, not a domain invariant.
- `Source` and `EventType` are enums. A new source always needs a new adapter anyway, so an open
  string type would only add typo risk. `SYNTHETIC` exists from day one so load-test traffic can
  never be mistaken for real data.
- `EventSink` is the port ingestion publishes into, with an idempotency contract. It's the seam
  where PostgreSQL (Phase 3) or Kafka (maybe Phase 8) plugs in without touching adapters.

**`trend` owns "what is counted".**

- `Topic(kind, value)` is the counting key, normalized on construction so equality equals identity
  for counting purposes ("  Rust" = "rust").
- `Mention(topic, eventId, source, occurredAt)` is one occurrence. It keeps the event id (so a trend
  can show the events behind it: the "why") and the source (so agreement between sources can be scored).
- `MentionExtractor` is only an interface for now; implementations come in Phase 5.

**Package rules are enforced by tests, not by convention.** `ArchitectureTest` (ArchUnit) checks
that `event` only uses the JDK, that `ingestion` and `trend` don't know about each other or the
API, that nothing outside `infrastructure` depends on it, and that packages have no cycles. I
verified it by adding a Spring import to `event`; the build failed with a readable message.

## Alternatives considered

- **Layered packages (`controller/`, `service/`, `repository/`).** Rejected: they group code by
  technical role, so one feature is spread across every package and nothing marks a boundary.
- **Random UUID or database sequence as id.** Rejected: the same item fetched twice would get two
  ids, and deduplication would need a separate lookup on `(source, externalId)` anyway.
- **Storing `id` as a record field.** Rejected: it could disagree with `source`/`externalId`.
  Deriving it makes that impossible.
- **`Mention` in `event`.** Rejected: which topics an event contains is an analysis decision that
  will change often; the event itself is a fact that doesn't.
- **Spring Modulith** instead of hand-written ArchUnit rules. It's a good fit for modular monoliths,
  but explicit rules are easier to read and explain at this stage. Worth revisiting if the rules grow.
- **JPA annotations on `PulseEvent`.** Rejected: that would tie the domain to the persistence
  technology. Phase 3 maps it in `infrastructure` instead.

## What happened

15 tests pass: domain invariants (identity, normalization, immutability, lateness), topic
normalization, the five architecture rules, and the Spring context test.

## Measurements

None yet. There's nothing to measure until data flows.

## Consequences / open questions

- `attributes` is `Map<String, String>`. Simple, but numeric values like scores are stringly typed.
  Revisit if trend detection needs them (e.g. weighting by HN score).
- Still open from 000: is an edited item a new event or an update of the same one? With derived
  ids, an edit of the same RSS item has the *same* id. Phase 3's insert policy (ignore vs. upsert)
  decides the outcome.
- `EventSink.accept` handles one event at a time. Batching will likely be the first change load
  testing forces.
- Next: Phase 2, an RSS adapter in `ingestion.rss` that produces `PulseEvent`s.
