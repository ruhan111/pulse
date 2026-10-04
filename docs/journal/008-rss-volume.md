# 008: How much RSS actually produces (Phase 3c)

**Date:** 2026-10-04 · **Phase:** 3c · **Commit/PR:** Phase 3c PR

## Context

3b added metrics, and with them an open question from 005: a real new-events-per-hour figure.
The first real run (005) saw 86 events in the first round and 0 new in the next minute, which
said nothing about the steady rate. Trend detection (Phase 5) only works if topics appear often
enough to stand out from their normal level, so this number decides what comes next.

## Decision

- **Measure with one real run on my machine**: the five shipped feeds, the 5-minute poll interval,
  everything as merged in 3b. I read the metrics endpoints at the end and took no other action.
- **Accept a short sample.** The run lasted 51 minutes on a Saturday night. A longer run would
  refine the number but not the conclusion (see below), so it isn't worth a day of waiting.
- **Conclusion: RSS alone is far too little volume for trend detection.** Phase 4 starts with a
  high-volume source (Wikipedia recent changes) before more feeds are added.
- **No Prometheus.** The counters reset on restart and have no history, but the main question,
  the rate, is answered by two readings or by the `events` table itself (`ingested_at` survives
  restarts). Nothing needed a time series yet.

## Alternatives considered

- **Run for 24 hours or more** to see the daily rhythm and per-feed rates. It's better data, but
  even a rate several times higher than measured would still be far too low (see measurements).
  It can be added as a follow-up entry if Phase 4 needs RSS numbers.
- **Add more RSS feeds instead of a new source.** Each feed adds roughly the same handful of items
  an hour. Twenty feeds would still be a trickle, and it would leave the questions that need volume
  (batching, pool size, decoupling ingestion from storage) unanswerable.
- **Start trend detection (Phase 5) on RSS anyway.** With about 12 items an hour across all
  topics, most topics appear once or twice, and two articles about the same thing would look like a
  100% spike. It would mostly detect noise.

## What happened

- **A restart remembered everything on real data.** The first reading after restarting the app
  (before this run) showed `pulse.events.accepted` = 86, all `DUPLICATE`: the whole first round was
  already stored. 3a's restart guarantee held on real feeds, not just in the end-to-end test.
- **`?tag=result:NEW` returned 404 until the first new event.** Micrometer creates a counter the
  first time it's incremented, and Actuator answers 404 both for an unknown metric and for a tag
  value that hasn't occurred yet. It looked like a broken endpoint. `availableTags` on the untagged
  metric shows which values exist. Creating the counters at 0 on startup would avoid the
  confusion; that hasn't been done yet.
- **New events with old dates.** Some new events were published days before Pulse stored them.
  The likely causes are ranked feeds (Hacker News front page, Lobsters), where a story appears
  when it climbs the ranking rather than when it was submitted, and editorial feeds re-promoting
  older articles. I haven't confirmed this with a per-channel query yet.
- **Hacker News timed out.** All 3 fetch failures were `TIMEOUT` on `hnrss.org/frontpage` (10 s
  deadline). No data was lost: a failed poll keeps the old cache validators, so the next round
  fetches the full feed.

## Measurements

One run, Saturday night 2026-10-03, 5 feeds, 5-minute interval:

| Measure | Value |
|---|---:|
| Uptime (`process.uptime`) | 3,058 s (51 min) |
| Events handed to the sink | 720 |
| New | **10** |
| Duplicates | 710 |
| **New events per hour** | **≈ 12** (all 5 feeds together) |
| Duplicates per new event | ≈ 70 |
| Rounds (estimated from uptime) | ≈ 10–11 |
| Fetch failures | 3, all `TIMEOUT` on Hacker News |
| Hacker News polls failing (estimated) | ≈ 3 of 10–11, about 30% |

- **About 12 new events an hour is the headline.** For comparison, the first round of a fresh
  database is 86 events, so a restart's backlog equals about 7 hours of live traffic. Phase 5 must
  not mistake that backlog for a spike (noted in 005).
- **98.6% of sink calls are duplicates.** That's the three feeds without conditional GET (Lobsters,
  Ars Technica, BBC) re-sending their full contents every round. At about 0.3 ms per duplicate
  insert (006) it costs well under a second per hour, so it's harmless here.
- **The Hacker News failure rate is based on about 10 polls**, and the poll count per feed is an
  estimate from uptime, not read from the metric. It's not enough to act on.
- **Caveat:** a weekend night is likely the quiet end of the daily cycle. A weekday rate could be
  several times higher. That's still too little for the conclusion to change.

## Consequences / open questions

- **Phase 3 is done**: events are stored once, survive restarts, and their flow is measurable.
  Phase 2's pending real-feed run is covered by 005 and this entry.
- **Phase 4 starts with Wikipedia recent changes**, a continuous stream of edits across all
  topics. It brings the volume that makes the open questions measurable: one insert per event,
  autocommit, the single poller thread, pool size, and the 30 s connection timeout.
- **Phase 5 has to choose which timestamp counts.** For ranked feeds like Hacker News, the time
  Pulse first saw a story (it reached the front page) may be more meaningful than its submission
  date. For news articles, the publish date is right. Undated items (007) are part of the same
  question.
- **Hacker News timeouts:** if the rate stays near 30% over a longer run, a longer deadline for
  slow feeds or a single retry is the small, justified fix that 004 left open.
- **Counters at 0 on startup** would remove the confusing 404s. It's a small change to make when
  the metrics code is next touched.
- Open for a later run: per-feed rates and the daily rhythm, from
  `select date_trunc('hour', ingested_at), count(*) from events group by 1 order by 1`.
