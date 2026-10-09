# 014: 012's day, recounted with MediaWiki's revert tags (Phase 5)

**Date:** 2026-10-09 · **Phase:** 5 (between 5a and 5b) · **Commit/PR:** journal 014 PR

## Context

012 measured a day of English Wikipedia edits and removed vandalism with a rule on edit summaries.
013 then showed that rule catches only about a quarter of reverts (3 of 12 in the same 3 minutes),
so 012's cleaned numbers were too high, by an amount that wasn't known yet. 013 added MediaWiki's
own revert tags as annotations, plus a one-time procedure to replay the tag stream for 012's day,
which Wikimedia keeps for at least 8 days.

## Decision

- **Run the replay on my machine** with the 5a version, starting the tag stream at 2026-10-07 18:20
  UTC. Done on 2026-10-09: Pulse started at 19:23 UTC and resumed from the edited checkpoint.
- **Recount exactly 012's window** (7 Oct 18:26:24 to 8 Oct 18:24:02 UTC) with distinct editors per
  page per clock hour, dropping every edit annotated `REVERT` or `REVERTED`. `REDIRECT` stays in,
  so the numbers stay comparable to 012.

```sql
with clean as (
  select e.title, date_trunc('hour', e.occurred_at) as hour, e.attributes->>'user' as editor
  from events e
  where e.source = 'WIKIPEDIA'
    and e.occurred_at >= '2026-10-07 18:26:24+00' and e.occurred_at <= '2026-10-08 18:24:02+00'
    and not exists (select 1 from event_annotations a
                    where a.event_id = e.id and a.kind in ('REVERT', 'REVERTED'))
),
per_page_hour as (
  select title, hour, count(distinct editor) as editors from clean group by 1, 2
)
select editors, count(*) as page_hours from per_page_hour group by editors order by editors;
```

## Alternatives considered

- **Adding the recount to 013.** That was the first plan, and the commit existed. But 013 had been
  merged by then, and merged entries aren't rewritten, so it became this entry.
- **Waiting for a fresh, live-annotated day.** That would have cost a day and lost the direct
  comparison with 012's numbers and pages.

## What happened

- **The replay procedure from 013 worked on a real installation.** The first attempt failed,
  because I pasted the statement into an interactive `psql` session where the `:since_ms`
  variable wasn't defined (a syntax error at `:`, nothing changed). With `\set since_ms
  1791397200000` first, it updated the row. The saved position then held the two recent-change
  topics unchanged and both tag topics at 1791397200000.
- **The ≥ 5 tier shrinks from 14 pages to 11, and the 3 that drop out are exactly the 3 that 012
  marked as doubtful:**

| Dropped page | Editors (summary rule) | 012's note |
|---|---:|---|
| Pneumonic plague | 7 | "unclear, possibly news" |
| James Ramsay (abolitionist) | 5 | "unclear (main-page feature? editing event?)" |
| Carrie (miniseries) | 5 | "probably a release or premiere" |

  Their editors were reverts and reverted edits that the summary rule missed.
- **The 11 pages that remain make about 7 stories,** each with a plausible real-world cause (my
  reading of title and timing, not verified):
  - Anne Carson;
  - Nana Patekar;
  - the MLB playoffs (NLCS, postseason, ALDS);
  - Hurricane Isaias (season, Hurricane…, Tropical Storm…);
  - Digger (2026 film);
  - the AFC U-17 Women's Asian Cup qualifiers;
  - Skydance Corporation.

  Judged by eye, none is noise.
- **The summary rule also removed genuine edits.** Anne Carson's peak hour has 29 editors with tags
  and 28 with the summary rule: its size-matching half paired a genuine edit with an unrelated one.
  Nana Patekar goes from 23 to 22, and Anne Carson's hours with ≥ 5 editors from 3 to 2, as
  revert-related edits drop out.

## Measurements

Page-hours by distinct editors, same window, three ways:

| Editors | Raw (012) | Summary rule (012) | MediaWiki tags |
|---|---:|---:|---:|
| 1 | 57,350 | 56,052 | 53,811 |
| 2 | 2,593 | 1,298 | 879 |
| 3 | 250 | 108 | 80 |
| 4 | 82 | 35 | 26 |
| 5–9 | 44 | 21 | 18 |
| ≥ 10 | 4 (11, 11, 29, 30) | 3 (11, 23, 28) | 2 (22, 29) |
| **≥ 3** | **380** | **167** | **126** |
| **≥ 5** | **48** | **24** | **20** |
| ≥ 3 per hour (÷ 24) | 15.8 | 7.0 | 5.3 |

- **The summary rule left about a quarter of the noise.**
  - ≥ 3 editors: 167 → 126 (−25%).
  - ≥ 5 editors: 24 → 20 (−17%).
  - 2 editors: 1,298 → 879 (−32%).
- **Against raw counts, tags remove 67% of the ≥ 3 candidates and 58% of the ≥ 5 ones.**
- **98.2% of active page-hours have a single editor** (53,811 of 54,816).

## Consequences / open questions

- **Correction to 012:**
  - **≥ 3 editors:** about 5 candidates an hour, not 7 (126 page-hours, not 167);
  - **≥ 5 editors:** 20 page-hours, not 24;
  - **the strongest tier:** 11 pages, not 14.

  012's raw numbers, its daily rhythm and its hand inspection stand.
- **The benchmark for 5c:** "≥ 5 genuine editors in an hour" gives about 20 page-hours, 11 pages and
  about 7 stories on this day, with no visible false positives. A baseline has to add real stories
  from the 3–4 editor tier (106 page-hours) without letting noise back in.
- **The replay was more complete than live operation.** It saw every `REVERTED` up to the day after
  the window, including late reverts. Live, a vandal edit counts until its revert arrives. 5b has
  to retract such edits, and its numbers will be somewhat noisier in the first minutes of a page's
  activity than these.
- **Still one weekday.** And story grouping (the hurricane and baseball each span 3 pages) remains
  open.
