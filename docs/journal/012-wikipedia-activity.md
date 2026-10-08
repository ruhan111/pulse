# 012: What a day of Wikipedia edits looks like (Phase 5, before coding)

**Date:** 2026-10-08 · **Phase:** 5 (opening measurement) · **Commit/PR:** Phase 5 measurement PR

## Context

Phase 4 closed (011) with English Wikipedia at about 3,600–4,700 stored edits an hour. Before
building trend detection, one question had to be answered with data: **is English Wikipedia (with
RSS's 12 items an hour) enough to find real trends?** The total volume doesn't answer that. What
matters is how edits are spread across pages: how often does a page get several editors in one
hour, and are those pages interesting or noise?

## Decision

- **Measure on a real 24-hour run** on my machine: the current `main`, unchanged. Laptop sleep
  was allowed, because the stream resumes from its checkpoint (010, 011).
- **Count distinct editors per page per clock hour**, by `occurred_at`. Editors rather than edits,
  because one person saving ten times is not ten people's interest (009 already saw one user make
  3 of 24 kept edits in 7 s).
- **First look at the busiest pages by hand**, then apply a rule for vandalism and reverts, and
  compare the counts before and after.
- **Keep the analysis in SQL against the stored events.** No code changes. These queries are
  one-off analysis, so they live here rather than in `src/main/resources/sql/`.

## Alternatives considered

- **A 24-hour run in the cloud sandbox.** The container is reclaimed when idle (it restarted twice
  during 4c), and the database volume goes with it. A run there would have needed hourly
  supervision and would have had gaps. A laptop that sleeps loses nothing, because Pulse catches up
  on waking.
- **Adding more sources before measuring** (pageviews, Hacker News, Mastodon). That's more
  ingestion code before knowing whether the existing source is enough or what detection needs from
  the others. Kept as the follow-up if detection turns out to miss things (see below).
- **Counting edits instead of editors.** It inflates single busy editors and vandal/revert pairs.

## What happened

**The first hour (18:26–19:04 UTC), inspected by hand.** 7 pages had 3 or more editors:

| Page | Editors | What the edits were |
|---|---:|---|
| South Fayette Township School District | 5 | 2 vandals, 3 people reverting, then page protection (`{{pp-vandalism}}`) |
| Mangifera indica | 4 | two blankings (−1,498 bytes), each reverted (+1,498) |
| Grise Fiord | 4 | "important info", "nuthin", each reverted within 0–3 min |
| Gerrymandering | 4 | a blanking and a joke edit, each reverted |
| Deaths in 2026 | 4 | adding recent deaths (always busy) |
| LeBron James | 3 | a content dispute and new material ("Philadelphia 76ers (2026–present)") |
| Major film studios | 3 | the Paramount Skydance / Warner Bros. Discovery acquisition |

- **4 of 7 were vandalism plus its clean-up**, so only 2 of 7 were plausible trends.
- **Every vandal edit was undone, and it shows in our data.** The revert has a summary such as
  "Reverted edits by …", "Reverted 1 edit by …" or "Reverting possible vandalism by …", its
  `size_delta` is the exact negative of the edit it undoes, and it follows within 0–12 minutes.
- **ClueBot NG, an anti-vandalism bot, arrives with `bot: false`**, so 009's bot filter doesn't
  catch it. Its edits are reverts, so a revert rule does.
- **The current hour keeps filling.** The same query run twice showed "Deaths in 2026" with 3 and
  then 4 editors for the 18:00 hour. Detection has to handle a bucket that isn't complete yet.
- **4a's choice of the HTML-rendered comment pays off.** Revert summaries keep the reverted user's
  name in plain text, so a revert can be matched to the edit it undid.

**The revert rule used for the measurement** (approximate, in SQL):
- Drop every edit whose summary starts with "revert", "undid" or "rv".
- Drop every edit that such a revert undid: same page, within 30 minutes, exactly opposite
  `size_delta`.

```sql
with e as (
  select id, title, occurred_at, attributes->>'user' as editor,
         (attributes->>'size_delta')::int as delta, summary
  from events where source = 'WIKIPEDIA'
),
reverts  as (select * from e where summary ~* '^(revert|undid|rv\M)'),
reverted as (
  select distinct e.id from e join reverts r on r.title = e.title
    and r.occurred_at between e.occurred_at and e.occurred_at + interval '30 minutes'
    and r.delta = -e.delta and r.id <> e.id
),
clean as (select * from e where id not in (select id from reverts) and id not in (select id from reverted))
select title, date_trunc('hour', occurred_at) as hour, count(distinct editor) as editors
from clean group by 1, 2;
```

**My first distribution query was wrong.** It used `count(distinct …) over ()`, which PostgreSQL
doesn't support in a window function. It was replaced with a subquery.

**The strongest pages of the day** (≥ 5 genuine editors in an hour after the revert rule): 14
pages, which looks like about 10 stories. The causes are my reading of title and timing, not
verified:

| Story | Pages | Peak editors | When (UTC) | Likely cause |
|---|---:|---:|---|---|
| Anne Carson | 1 | 28 | Thu 11:00–16:00 | probably the Nobel Prize in Literature, announced Thursday 13:00 Stockholm time (11:00 UTC) |
| Nana Patekar | 1 | 23 | 02:00–12:00, 6 h | a story in India (Indian daytime) |
| MLB postseason | 3 | 9 | 03:00–05:00 | evening games in the US |
| Hurricane Isaias | 3 | 7 | 00:00–04:00 | a storm strengthening; page apparently renamed from "Tropical Storm" |
| Skydance Corporation | 1 | 5 | 05:00 | continues the acquisition story from the first hour |
| Pneumonic plague | 1 | 7 | Wed 20:00 | unclear, possibly news |
| Digger (2026 film), Carrie (miniseries) | 2 | 6, 5 | 23:00, 05:00 | probably a release or premiere |
| 2027 AFC U-17 Women's Asian Cup qualification | 1 | 5 | 15:00 | match results |
| James Ramsay (abolitionist) | 1 | 5 | 09:00 | unclear (main-page feature? editing event?) |

- **No vandalism among the 14.** About 9 of the 10 stories look like genuine real-world events.
- **One story often spans several pages:** the hurricane appears 3 times and baseball 3 times.
- **A rename splits a topic.** Renames are `log` events, which 4a skips as `NOT_AN_EDIT`.
- **"Deaths in 2026" never reaches 5 editors in an hour**, so pages that are always busy matter
  mainly in the 3–4 editor tier.
- **The second-biggest story was Indian**, and its hours follow Indian daytime. English Wikipedia
  is not only American or British news.

## Measurements

The run on my machine, Wednesday 2026-10-07 18:26:24 to Thursday 2026-10-08 18:24:02 UTC (23 h
58 min). 86,193 edits stored, no hour missing.

**Daily rhythm** (stored edits per clock hour):

| | Edits/hour | When (UTC) |
|---|---:|---|
| Quietest | 2,690 | 04:00 |
| Busiest | 4,744 | 20:00 |
| Average | 3,597 | 86,193 edits / 23.96 h |
| Peak band | 3,924–4,744 | 13:00–21:00 |
| Low band | 2,690–3,066 | 04:00–10:00 |

The busiest-to-quietest ratio is 1.8×. 20:00 Wednesday (4,744) matches 011's 20:00 Tuesday
(≈ 4,700).

**Distinct editors per page per clock hour**, raw and after the revert rule (page-hours; "per
hour" is divided by the 24 hours of the run):

| Editors | Raw | Without reverts | Per hour, without reverts |
|---|---:|---:|---:|
| 1 | 57,350 | 56,052 | 2,336 |
| 2 | 2,593 | 1,298 | 54 |
| 3 | 250 | 108 | 4.5 |
| 4 | 82 | 35 | 1.5 |
| 5–9 | 44 | 21 | 0.9 |
| 11 / 23 / 28 / 29 / 30 | 4 | 3 | |
| **≥ 3** | **380** | **167** | **≈ 7** |
| **≥ 5** | **48** | **24** | **≈ 1** |

- **97.5% of active page-hours have a single editor**, so several editors in an hour are rare.
- **The revert rule halves the 2-editor bucket** (vandal + reverter is the typical pair), and
  removes 56% of the ≥ 3 candidates and 50% of the ≥ 5 ones.
- **The ≥ 5 tier has 24 page-hours**, which are the 14 pages above.

## Consequences / open questions

- **English Wikipedia is enough to build detection on.** There's a quiet background, about 7
  candidate page-hours an hour after removing vandalism, and about one strong one an hour. More
  sources stay a follow-up, to be driven by what detection misses. 5d's evaluation must list
  missed stories, not only precision.
- **The revert rule belongs in 5a,** not as SQL but in code:
  - pair a revert with the edit it undid (summary pattern; opposite `size_delta`, same page,
    within a window);
  - neither half counts as interest; both are counted as a reason, the way skip reasons are.
  - Before building it, check whether Wikimedia's revert tags (`mw-reverted`, `mw-rollback`, …)
    are available on a stream. If so, they're more reliable than summary text.
- **The benchmark to beat:** "≥ 5 genuine editors in an hour" already gives a short, precise list.
  The baseline in 5c has to show it adds something: catching usually silent pages at 3–4 editors,
  and suppressing pages that are always busy.
- **Use a sliding window rather than clock hours.** Clock hours split stories and leave the
  current hour incomplete. A 28-editor story has its first editors within minutes, so the 5-minute
  freshness SLO looks reachable.
- **For later:** grouping pages into one story (the hurricane, baseball), following renames, and
  temporary accounts (`~2026-…`) that may make one person look like several.
- **This is one weekday.** Weekends and big news days may differ; the detector's thresholds should
  be checked against a second day before trusting them.
