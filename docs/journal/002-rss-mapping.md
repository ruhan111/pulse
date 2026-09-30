# 002: Mapping RSS entries to events (Phase 2a)

**Date:** 2026-09-30 · **Phase:** 2a · **Commit/PR:** Phase 2a PR

## Context

Phase 2 is split into four parts: mapping (2a), fetching (2b), polling (2c) and running against
real feeds (2d). This first part is pure logic: take a parsed feed entry and produce a valid
`PulseEvent`, or explain why it can't. No network, no Spring, no scheduling.

## Decision

All code lives in `ingestion.rss`:

- **`RssFeedParser`**: bytes → Rome's `SyndFeed`. Rome handles RSS 0.9x/1.0/2.0 and Atom behind one
  model. DOCTYPEs are rejected explicitly, which blocks XML external entity (XXE) attacks.
- **`RssEventMapper`**: one `SyndEntry` → `MappedEntry`, which is either `Mapped(event)` or
  `Skipped(reason, detail)`.
- **`SkipReason`**: `MISSING_TITLE`, `MISSING_LINK`, `INVALID_LINK`. An enum so skips can be
  counted as a metric later.

Mapping rules:

| Field        | Rule |
|--------------|------|
| `externalId` | guid (RSS) / id (Atom), else the link. Rome already applies this fallback in `getUri()`. |
| `occurredAt` | published, else updated, else `ingestedAt` |
| `ingestedAt` | injected `Clock`, so tests control time |
| `title`      | HTML → text with Jsoup; entries without a title are skipped |
| `url`        | resolved against the feed URL; only absolute http(s) is accepted |
| `summary`    | description, else first content; HTML → text; truncated to 1,000 chars |
| `attributes` | author and categories, when present |

## Alternatives considered

- **Throwing an exception for bad entries.** Rejected: bad entries are *normal* in real feeds, not
  exceptional. A result type makes skipping explicit, keeps the caller free of try/catch, and makes
  skip reasons countable.
- **Returning `Optional<PulseEvent>`.** Simpler, but it loses the reason, and 2d wants to report
  how many entries were skipped and why.
- **Regex for stripping HTML.** Rejected: regex can't handle entities, `<script>` contents or
  malformed markup correctly. Jsoup is small and does it properly.
- **Writing my own RSS/Atom parser.** Rejected: supporting four format variants teaches nothing
  about system design.

## What happened

All tests passed on the first run, which was suspicious, so I checked two things directly against
Rome instead of trusting the sample files:

1. **The XXE test failed for the right reason.** Rome reported "DOCTYPE is disallowed", not some
   unrelated parse error.
2. **Rome turns non-URL guids into links.** An RSS item with `<guid>article-1006</guid>` (where
   `isPermaLink` defaults to `true`) and no `<link>` comes out of Rome with `link = "article-1006"`.
   My mapper would then resolve it against the feed URL and invent
   `https://news.example.com/article-1006`, a page that doesn't exist. Fixed: a guid copied into
   the link only counts if it's already an absolute URL. Two sample items now cover both cases.

Lesson: sample files only test what I already know about. Checking the library's actual behaviour
found a bug the samples couldn't.

## Measurements

None yet. 32 tests pass (13 mapper, 4 parser).

## Consequences / open questions

- `externalId` depends on feeds keeping their guids stable. If a feed changes guid format, every
  item looks new. There's nothing to do about that until it's observed in 2d.
- Future-dated `occurredAt` values are accepted unchanged. Decide in Phase 5 if they distort counts.
- `type` is always `PUBLISHED`. Detecting edits needs stored state (Phase 3).
- Next: 2b, the HTTP fetcher.
