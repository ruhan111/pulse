# 009: Mapping Wikipedia recent changes (Phase 4a)

**Date:** 2026-10-04 · **Phase:** 4a · **Commit/PR:** Phase 4a PR

## Context

008 measured about 12 new RSS events an hour, far too few for trend detection, and chose
Wikipedia recent changes as Phase 4's high-volume source. Phase 4 is split like Phase 2:

- **4a** (this entry): mapping
- **4b**: the stream client
- **4c**: wiring and a measured real run
- **4d**: whatever 4c's numbers justify

This part is pure logic: take one event from Wikimedia's `recentchange` stream and produce a
`PulseEvent`, or say why not.

Decisions made before coding:

- **The source is EventStreams (SSE)**, not polling the MediaWiki API. 4b will cover the reasons.
- **Only English Wikipedia** (`enwiki`) is kept.
- **Bot edits are skipped** at ingestion.
- **Wikipedia page titles as a topic kind** (`TopicKind.PAGE`) waits for Phase 5.

## Decision

All code lives in `ingestion.wikipedia`:

- **`RecentChangeMapper`**: one JSON object (a `String`) → `MappedChange`, which is either
  `Mapped(event)` or `Skipped(reason, detail)`, like RSS.
- **`SkipReason`**, in the order the checks run, cheapest and most common first:

| Order | Check | Reason |
|---|---|---|
| 0 | invalid JSON, or a needed field missing or of the wrong type | `MALFORMED` |
| 1 | `wiki` ≠ `enwiki` | `OTHER_WIKI` |
| 2 | `type` not `edit` or `new` (log, categorize, external) | `NOT_AN_EDIT` |
| 3 | `namespace` ≠ 0 (talk, user, category, draft…) | `NOT_ARTICLE` |
| 4 | `bot: true` | `BOT` |

Mapping rules:

| Field | Rule |
|---|---|
| `source` | `WIKIPEDIA` (already in the enum since Phase 1) |
| `channel` | `wiki`, i.e. `enwiki` |
| `externalId` | `enwiki:<revision.new>`. Revision ids are unique per wiki and never reused. |
| `type` | `new` → `PUBLISHED`, `edit` → `EDITED` |
| `occurredAt` | `timestamp` (Unix seconds, when the edit was saved) |
| `title` | the page title |
| `url` | `meta.uri`, the article rather than the diff: trends are about pages |
| `summary` | the edit comment as plain text: `parsedcomment` (HTML) through Jsoup, else the raw `comment` |
| `attributes` | `user`, `minor`, `size_delta` (new − old length; a new page counts from 0), each only when present |

- **"enwiki" is a constant, not configuration.** A list in `application.yaml` would be configuration
  nobody needs yet. Turning the constant into a setting is a small change when a second wiki is wanted.
- **Each check reads only the fields it needs.** Log and categorize events have no `revision`. If
  the id were read first, they would all look `MALFORMED` instead of `NOT_AN_EDIT`.
- **Fields are read with JSON pointers** (`/revision/new`), so a skip's detail names the full path of
  the missing field.
- **Strict types.** `"namespace": "0"` or `"bot": "false"` is `MALFORMED`, not guessed. A lenient
  `bot` default of `false` would let bots in whenever the field went missing.
- **Optional extras never cause a skip.** An edit without `user`, `minor` or `length` is still an
  edit.
- **No truncation of summaries.** MediaWiki limits edit summaries to 500 characters. The longest
  comment in the sample was 349.
- **Test data is real.** I recorded 30 seconds of the live stream on 2026-10-04 at 10:34 UTC and
  committed it as `src/test/resources/wikipedia/recentchange-sample.jsonl`: 859 events, 1.1 MB, the
  `data:` lines only. The single-case fixtures are events copied from it. Malformed cases are real
  events with one field removed or changed in the test.

New dependency: none in practice. `tools.jackson.core:jackson-databind` (Jackson 3.1.5, managed
by Spring Boot) was already on the classpath through `spring-boot-starter-webmvc`. It's now
declared in `pom.xml` because ingestion uses it directly and shouldn't depend on the web starter
by accident.

## Alternatives considered

- **A separate parser and a `RecentChange` record** (like `RssFeedParser` → `SyndEntry` → mapper).
  Rejected because the event kinds have different fields: log events have no revision or length,
  categorize events have no revision. A record would need nullable fields for nearly everything, and
  the "is this field required?" question would move into the parser, which can't know the answer
  before the filters have run. Rome needed its own parser because it reads a whole document of many
  entries. Here one line is one event.
- **Binding JSON straight onto a record with Jackson annotations.** It's less code, but missing
  fields would silently become `null`/`0`/`false`, and `bot` defaulting to `false` is exactly the
  wrong failure. It would also put Jackson annotations on our types.
- **Sharing `MappedEntry`/`SkipReason` with RSS.** The shape is the same but the reasons aren't. A
  common type would need a shared package in `ingestion` and a reason type covering both sources.
  Two small sealed interfaces are cheaper than that abstraction. I'll reconsider when a third source
  arrives.
- **A configurable list of wikis.** See above. It was in the first plan and dropped on review.
- **Using the rc `id` as `externalId`.** It would work for edits. It's missing on 11 of the 859
  sampled events, but all of those are log entries, which are skipped anyway. The revision wins
  because it's what Wikipedia itself links to (`?oldid=…`).
- **The diff URL (`notify_url`) as `url`.** It's more precise for one edit, but trend detection
  groups by page, and 100 edits of one page should point at one place.
- **`comment` instead of `parsedcomment` as summary.** The raw comment is wikitext:
  `added [[Category:Naval ships commissioned in 2022]] using [[WP:HC|HotCat]]`. The parsed one, as
  text, reads `added Category:Naval ships commissioned in 2022 using HotCat`. Jsoup was already a
  dependency.

## What happened

- **The sandbox could not reach Wikimedia at first.** The proxy rejected `stream.wikimedia.org` and
  `en.wikipedia.org`. After `stream.wikimedia.org` was allowed in the cloud environment's network
  settings, `curl -sN --max-time 30` recorded the sample. Only the stream host is needed: each event
  carries its article URL, so Pulse never calls `en.wikipedia.org`.
- **The SSE `id:` is not a simple number.** Each `id:` line is a JSON array of Kafka positions, e.g.
  `[{"topic":"eqiad.mediawiki.recentchange","partition":0,"timestamp":1791110060549}, …]`. 4b has
  to store and resend it as an opaque string.
- **The first 16 tests all passed on the first run.** That was suspicious (see 002), so I broke the code twice:
  - *Removed the wiki check:* 2 tests failed, `skipsOtherWikis` and the whole-sample test. Without
    the check, the sample keeps 174 events from 41 wikis instead of 24 from enwiki.
  - *Read the revision before the type check:* 2 tests failed, `skipsLogEntriesAndCategoryChanges`
    and the whole-sample test. Log and categorize events turned into `MALFORMED`.
  - Restored both times, and green.
- **Jackson 3 behaviour, checked directly** rather than assumed:
  - `readTree("")` returns a `MissingNode` instead of throwing. Every field lookup on it is missing,
    so it ends up `MALFORMED: missing /wiki`, the same as `[]` and `null`.
  - Trailing garbage after a complete object (`{} trailing`) is rejected. Jackson 3 enables
    `FAIL_ON_TRAILING_TOKENS` by default, while Jackson 2 silently ignored it.
- **A review of the finished code found one bug the tests missed.** `Instant.ofEpochSecond` throws
  `DateTimeException` for a timestamp outside `Instant`'s range, e.g. `Long.MAX_VALUE`. That isn't
  an `IllegalArgumentException`, so it escaped the mapper. In 4b it would have stopped the stream
  reader on one bad event. It's now caught as `MALFORMED`, with a test.
- **Two things in the data that matter for Phase 5:**
  - **Both new pages in the sample were redirects** ("Redirected page to [[USS Whirlwind (PC-11)]]").
    A redirect is a new title, not new content. They may need to be recognised as such (e.g. by
    `size_delta` or the comment) before counting them.
  - **One user made 3 of the 24 kept events, all on one new page, within 7 seconds** (RBNS
    Al-Sakeer). Counting edits per page would call that activity. Counting distinct editors would
    not. That's why `user` is kept as an attribute.

## Measurements

From the 30-second sample (one Sunday morning, 10:34 UTC, 29.3 s from first to last event):

| Measure | Value |
|---|---:|
| Events on the stream (all wikis) | 859 (≈ **29/s**) |
| Bytes of `data:` lines | 1.1 MB (≈ 39 KB/s, ≈ 3.4 GB/day) |
| enwiki events | 78 (9%) |
| Kept after all filters | **24 (≈ 0.8/s, ≈ 2,900/hour)** |
| Skipped `OTHER_WIKI` | 781 (91%) |
| Skipped `NOT_AN_EDIT` | 34 |
| Skipped `BOT` | 13 |
| Skipped `NOT_ARTICLE` | 7 |
| `MALFORMED` | 0 |

- **About 2,900 events an hour is about 250× RSS** (008: ≈ 12/hour). At 0.39 ms per insert (006),
  0.8 events/s cost about 0.3 ms of database time per second, 0.03% of one thread. Batching
  isn't needed at this rate.
- **What we throw away dominates.** 97% of the events downloaded and parsed are skipped. The cost is
  bandwidth and JSON parsing, not the database. 4c should measure the CPU cost of parsing about 29
  events/s.
- **Storage:** about 2,900 rows an hour is about 70,000 a day and 25 million a year. That's fine
  for PostgreSQL, but it's the first time retention is a real question.
- **Caveat:** 30 seconds on a Sunday morning is one sample. The rate varies over the day and between
  weekdays. 4c measures it properly.
- **Test suite:** 88 tests (17 new), all passing.

## Consequences / open questions

- The mapping is done and tested on real data. Nothing runs yet: there's no client and no wiring.
- **4b: the SSE client.**
  - Connect, read `event:`/`id:`/`data:` lines, and handle the `:ok` comment line the stream starts
    with.
  - Reconnect with backoff and `Last-Event-ID`, storing the last id in a small table so a restart
    resumes where it stopped.
  - Run on a dedicated thread, separate from the RSS poller.
  - The recorded capture can be replayed from a local `HttpServer`.
- **4c** measures the real rate over hours, parsing cost, lateness (`meta.dt` vs. `timestamp`),
  and storage growth.
- **Phase 5 questions:** redirects, one editor vs. many, and `TopicKind.PAGE`.
