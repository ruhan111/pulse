# Pulse journal

A chronological record of how Pulse was built. The README describes what Pulse *is*; the journal
describes how it *got there*.

## Rules

- One entry per milestone or significant decision, not per commit.
- Entries are append-only. If a later entry proves an earlier one wrong, write a new entry that
  says so and link back. Being wrong on the record is the point.
- Every infrastructure addition must reference the entry that measured the problem it solves.
- Numbers beat adjectives: "p99 insert latency went from 4 ms to 180 ms at 2,000 events/sec", not
  "the database got slow".

## Entry template

```markdown
# NNN: Title

**Date:** YYYY-MM-DD · **Phase:** N · **Commit/PR:** link

## Context
Where the system stands and what problem or question prompted this step.

## Decision
What I did.

## Alternatives considered
What else I could have done and why I didn't.

## What happened
What worked, what broke, what surprised me.

## Measurements
Numbers, graphs, load-test results (if any).

## Consequences / open questions
What this makes easier or harder, and what's next.
```

## Entries

| #   | Title                                          | Phase |
|-----|------------------------------------------------|-------|
| 000 | [Why Pulse, and why start small](000-why-pulse.md) | 0     |
| 001 | [The domain model and package boundaries](001-domain-model.md) | 1     |
