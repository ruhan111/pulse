# 013: Reverts from MediaWiki's own tags (Phase 5a)

**Date:** 2026-10-08 · **Phase:** 5a · **Commit/PR:** Phase 5a PR

## Context

012 showed that vandalism and its clean-up are the biggest source of false trends: 4 of the 7
busiest pages in its first hour were vandal edits and their reverts. It removed them with a rule
on edit summaries: drop an edit starting with "revert", "undid" or "rv", plus the edit it undid.

The plan for 5a was to build that rule into the mapper. Before writing it, I checked whether
MediaWiki's own revert tags (`mw-undo`, `mw-rollback`, `mw-manual-revert`, `mw-reverted`) could be
read from a stream, because MediaWiki detects reverts by comparing content, not by reading what an
editor typed.

## Decision

- **Use MediaWiki's tags, not edit summaries.** They come from Wikimedia's
  `mediawiki.revision-tags-change` stream.
- **Read both streams over one connection:**
  `…/v2/stream/recentchange,mediawiki.revision-tags-change`.
  - The server interleaves them and keeps a position for every stream in one `Last-Event-ID`.
  - One thread, one checkpoint, and both streams always resume from the same point.
  - `ResumePosition` already rewinds every timestamp in the id, so it needed no change. A test
    pins the four-topic format.
  - `RecentChangeConsumer` became **`WikipediaStreamConsumer`**. It parses each event once and
    routes it by `meta.stream` (**`WikipediaStream`**) to `RecentChangeMapper` or the new
    **`TagChangeMapper`**.
  - **The checkpoint key stays `wikipedia.recentchange`.** Renaming it would lose the saved
    position of running installations.
- **Tags become annotations, not events.** A tag is a fact learned later about an edit Pulse
  already has.
  - The new port **`EventAnnotations`** and record **`EventAnnotation(eventId, kind, annotatedAt)`**
    live in `event`.
  - `Kind` is `REVERT` (the edit undid others), `REVERTED` (the edit was undone) or `REDIRECT`
    (the edit created a redirect: a new title, not new content, see 009).
  - A tag change carries the revision id, so the annotation's `EventId` is exactly that of the
    edit. `RecentChangeMapper.externalId(wiki, revision)` is the one place the id is built, and
    both mappers use it.
- **A new table `event_annotations(event_id, kind, annotated_at)`** (changeset 003, primary key
  `(event_id, kind)`, `insert … on conflict do nothing`).
  - **No foreign key to `events`.** A revert's tag can arrive before the revert itself (measured
    below), and `REVERTED` can point at an edit Pulse never saw.
  - The first record wins, so `annotated_at` stays the time MediaWiki first reported the fact.
- **`TagChangeMapper` details:**
  - Only English Wikipedia articles (other wikis and namespaces are skipped, with the same reasons
    as recent changes).
  - Only tags this change *added*: `tags` minus `prior_state.tags`. The event repeats every tag the
    revision already had.
  - Everything else is skipped with a new reason, `NO_RELEVANT_TAG`.
- **`JsonFields`:** the JSON-pointer helpers moved out of `RecentChangeMapper`, now that two
  mappers need them.
- **Metrics:**
  - `pulse.wikipedia.changes.received` and `.skipped` gained a `stream` tag.
  - New: `pulse.annotations.accepted` (`kind`, `result`), counted by the store, like
    `pulse.events.accepted`.
  - All are registered at 0. `StreamTally` counts per stream; a used event can have several
    outputs.
- **The disconnect log line reports both streams.**
- **`EventType.REVERTED` and the summary rule from the 5a plan are dropped.**

## Alternatives considered

- **The summary rule from 012, improved.** Dropping the `^` anchor and adding "Restored revision"
  would have caught about 10 of the 12 reverts below, but never manual reverts, which have
  free-text summaries. Every tool that writes its own summary would be a new pattern to chase.
- **A new `EventType.REVERTED`** set by `RecentChangeMapper` from the summary. The revert edit
  could be typed that way, but the edit that *was* reverted only becomes known later, so a type
  fixed at insert can't express it. Annotations cover both, and don't rewrite stored events.
- **A second consumer for the tag stream,** or a `StreamConsumer` extracted to share the loop.
  This was in the first revised plan. One connection makes it unnecessary, and keeps both streams
  at the same position after an outage.
- **Storing tags as `PulseEvent`s.** A tag is not something that happened at the source in its own
  right; it's about another event, and counting would have to tell the two apart again.
- **Upserting events** (setting a "reverted" column). It breaks first-version-wins (006), and fails
  when the tag arrives before the edit.
- **A foreign key from annotations to events.** It would reject every tag that arrives first, and
  every `REVERTED` for an edit older than Pulse's data.

## What happened

- **The investigation.**
  - I captured both streams separately for the same 3 minutes (2026-10-08 19:05–19:08 UTC).
    Among 212 English article edits Pulse kept, **the summary rule found 3 reverts and the tags
    found 12**, including those 3.
  - The 9 the summary rule missed:
    - 6 had a tool prefix ("Interceptor: Reverting …"), so `^revert` never matched;
    - 1 said "Restored revision …";
    - 2 were manual reverts with free-text summaries.
- **Timing.**
  - The tag on a revert arrives together with the revert: tags came 0–0.1 s *before* their edit
    (median for all tags: 18 ms before).
  - `mw-reverted` arrives when the revert happens: 10–37 s after the undone edit here, and up to
    12 minutes in 012's examples.
  - 92% of kept edits got some tag event.
- **One connection for both streams works.** HTTP 200; 203 recent changes and 71 tag changes in
  8 seconds; one id holding positions for all four topics (two streams, two datacenters).
- **Fixtures are real.** `two-streams.txt` holds 221 events from a 4-minute combined capture (172
  recent changes, 49 tag changes): the window in which an edit to "Torrington, Connecticut"
  (revision 1379235224) is rolled back (1379235231). In it the rollback's tag arrives three events
  before the rollback. The tag-mapper fixtures are single real events: reverted, rollback, undo,
  manual revert, new redirect, an irrelevant tag, another wiki, a talk page.
- **The end-to-end test** runs the whole app against that capture and finds `REVERTED` and
  `REVERT` next to the two stored edits in PostgreSQL.
- **Proving the tests** (each restored afterwards, then green):

| Break | Red |
|---|---|
| `prior_state` ignored | `onlyNewlyAddedTagsCount` |
| `mw-manual-revert` not mapped | `rollbackUndoAndManualRevertAreAllReverts` |
| The annotation's id built differently from the event's (`enwiki-` instead of `enwiki:`) | 4 tests, including the end-to-end test and `pointsAtTheSameEventAsTheRecentChangeOfThatRevision` |
| No routing: everything sent to the recent-change mapper | 3 consumer tests |
| A failed annotation swallowed instead of ending the connection | `aFailedAnnotationDoesNotAdvanceTheCheckpointPastIt` |

- **The replay procedure for stored history, tested against a real restart.** The procedure:
  1. Strip the tag topics from a saved position, which is what an installation from before 5a has.
  2. Add them back with an SQL update starting at an earlier time.
  3. Restart.

  The tag stream re-read the 10 minutes since that time within 5 s of reconnecting (7,542 tag
  events), and all 69 annotations from the first pass came back as already known. PostgreSQL
  rewrites the JSON with spaces and its own key order; Wikimedia accepted it.
- **My first replay SQL only added tag topics when none were there.** Once the new version had run
  even once, the saved position would already have tag topics positioned at "now", and the update
  would silently do nothing. The final version replaces any existing tag topics. I tested both
  cases on a real row.
- **Wikimedia answered HTTP 500 on the first connection after the replay update.** The identical
  request succeeded 1 s later. It's the second transient 500 seen (011), and the backoff covers it.

## Measurements

**Live run** in the sandbox: the packaged app against both streams, PostgreSQL from
`compose.yaml`, 2026-10-08 19:30:32–19:40:33 UTC (10 min).

| Stream | Received | Used | Skipped |
|---|---:|---:|---:|
| Recent changes | 23,459 (39/s) | 633 stored edits | 22,826 (21,646 other wikis) |
| Tag changes | 6,992 (11.7/s) | 69 annotations | 6,923 (5,979 other wikis) |

| Annotations | Count | Matching a stored edit of this run |
|---|---:|---:|
| `REVERT` | 29 | 29 (4.6% of stored edits) |
| `REVERTED` | 36 | 16 (2.5% of stored edits); 20 undo edits from before the run |
| `REDIRECT` | 4 | 2; probably bot redirects, which are skipped |

- **About 7% of the edits Pulse stores are reverts or reverted.** Those are exactly the edits that
  made quiet pages look busy in 012.
- **The extra cost of the tag stream** is about 35% more bytes (3.1 MB against 8.8 MB over 3
  minutes) and about 12 more events a second to parse, of which about 0.1 a second become an
  annotation.
- **Test suite:** 155 tests (20 new), all passing, `./mvnw clean test`.

## Consequences / open questions

- **Correction to 012.** Its "after reverts" numbers came from the summary rule, which catches
  only about a quarter of reverts (3 of 12 here). Its counts with reverts removed (167 page-hours
  with ≥ 3 editors, 24 with ≥ 5) are therefore **too high**, and its "about 7 candidates an hour"
  is an upper bound. The size of the correction is unknown until the stored day is annotated (next
  point). 012's raw numbers, the daily rhythm and the hand inspection stand.
- **Annotating the stored day: run once, before about 15 October.** Wikimedia keeps the tag stream
  for at least 8 days (010), so the tags for 012's day (from 2026-10-07 18:26 UTC) can still be
  read.
  1. Stop Pulse and update to this version.
  2. Run the statement below with `since_ms` = 2026-10-07 18:20 UTC (`1791397200000`).
  3. Start Pulse.

  The tag stream then replays the whole day: about 1 million tag events (11.7 a second × 24 h). At
  the catch-up speed measured here (more than 1,500 a second) that takes up to about 11 minutes,
  while recent changes continue from their own position.

  ```bash
  docker compose exec -T postgres psql -U pulse -d pulse -v since_ms=1791397200000 -f - <<'SQL'
  update checkpoints
  set position = (
          coalesce((select jsonb_agg(topic)
                    from jsonb_array_elements(position::jsonb) as topic
                    where topic->>'topic' not like '%revision-tags-change'), '[]'::jsonb)
          || jsonb_build_array(
              jsonb_build_object('topic', 'eqiad.mediawiki.revision-tags-change', 'partition', 0, 'timestamp', :since_ms),
              jsonb_build_object('topic', 'codfw.mediawiki.revision-tags-change', 'partition', 0, 'timestamp', :since_ms))
      )::text,
      updated_at = now()
  where stream = 'wikipedia.recentchange';
  SQL
  ```

  If the JSON were ever malformed, Wikimedia would answer 400, and Pulse would delete the
  checkpoint and start from now (010). That loses the replay, not stored data.
- **5b, counting,** can now count an edit only if it has neither `REVERT` nor `REVERTED`. It has to
  retract an edit when `REVERTED` arrives later, and it mustn't assume an annotation arrives after
  its event.
- **Open:**
  - Whether to count `REDIRECT` creations at all.
  - Temporary accounts (`~2026-…`) that may make one vandal look like several.
  - Whether bots that don't set the bot flag (ClueBot NG, Interceptor users) need anything beyond
    their edits being tagged as reverts.
