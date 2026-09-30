# 000: Why Pulse, and why start small

**Date:** 2026-09-30 · **Phase:** 0 · **Commit/PR:** initial planning

## Context

I want a hobby project for learning system design and architecture properly. That means being able
to explain why each component exists, what happens when it fails, and how the system behaves as
load grows, not just list technologies.

Pulse answers one question: *what is changing on the internet right now, and why?* It fits the
learning goal because the workload comes from the outside world rather than from users. The system
needs very little user input but can take an effectively unlimited amount of data input.

## Decision

Start as a **modular Spring Boot monolith** with PostgreSQL. No Kafka, Redis, Kubernetes,
microservices or frontend at the start.

- Packages by business responsibility: `event`, `ingestion`, `trend`, `api`, `infrastructure`.
- All sources normalize into one `PulseEvent` model.
- Ingestion publishes into an `EventSink` port rather than writing to the database directly. This
  is the seam where a message broker could later be introduced without touching source adapters.
- Start with RSS as the first source; it's simple, real and continuously changing.

## Alternatives considered

**Kafka-first pipeline** (connectors → raw topics → stream processing → trends). It's attractive
because it's "what real systems look like", and it gives replay and decoupling for free. Rejected
for now: I can't yet explain which measured problem Kafka would solve here. Starting with it would
teach me Kafka configuration, not why Kafka exists. It stays the leading candidate for Phase 8.

## What happened

Planning only. Review feedback that shaped the plan:

- **"Trend" needs a key.** Counting total events per hour measures volume, not trends. Something
  must be extracted from each event to count (terms, domains, repos). That became the `Mention`
  concept.
- **RSS alone is too low-volume** to detect anything statistically meaningful (3 vs 1 items per
  hour is noise). A high-volume source such as Wikipedia EventStreams moves into Phase 4. As a
  push-based source, it also tests whether the source abstraction handles both poll and push.
- **Event model additions:** `ingestedAt` next to `occurredAt` (needed for lateness and freshness
  metrics), `summary`, a per-source `attributes` field, and `(source, externalId)` as the dedup key.
- **"Evidence" needs targets.** Draft SLOs were written down (see README) so Phase 7 measurements
  can actually demonstrate a problem.
- **Metrics from Phase 3, not Phase 8.** You can't make decisions based on evidence without instrumentation.

## Measurements

None yet.

## Consequences / open questions

- Is an update to an existing item (HN score rising, RSS item edited) a new event, an upsert, or a
  separate snapshot stream?
- Late events: recompute closed time buckets, or drop them?
- Baselines: previous hour is fooled by daily cycles. Compare with the same hour on previous days, or use EWMA?
- Next: Phase 1, defining the domain model.
