# 003: Fetching feeds over HTTP (Phase 2b)

**Date:** 2026-09-30 · **Phase:** 2b · **Commit/PR:** Phase 2b PR

## Context

2a turned feed documents into events. 2b gets those documents from the internet. This is the first
code that talks to systems I don't control, so the design question isn't "how do I download a
file" but "what can a server do to me, and how do I stop it spreading into the rest of Pulse?"

## Decision

`FeedFetcher.fetch(url, previousValidators)` returns a `FetchResult`:

| Result        | When |
|---------------|------|
| `Fetched(body, validators)` | 200. `validators` holds the `ETag` / `Last-Modified` to send next time |
| `NotModified` | 304. The feed hasn't changed, so there's nothing to parse |
| `Failed(reason, detail)` | `TIMEOUT`, `NETWORK_ERROR`, `HTTP_ERROR` (any other status) or `TOO_LARGE` |

Protections:

- **One deadline for the whole fetch, body included.** `sendAsync(...).get(deadline)`, and on
  timeout the exchange is cancelled so no connection lingers in the background.
- **Size limit while downloading.** A custom `BodySubscriber` pulls the body one chunk at a time
  and cancels as soon as it exceeds the limit. Memory use is bounded whatever the server sends.
- **Conditional GET.** `If-None-Match` / `If-Modified-Since` are sent when we have validators, so an
  unchanged feed costs a tiny 304 instead of the full document.
- **Polite identification.** A `User-Agent` naming Pulse and linking to the repo, so server
  operators can see who is polling them.
- **Redirects** are followed, but never from https to http.

The fetcher is **stateless**. It doesn't remember validators; the caller passes them in. Where
per-feed state lives is then an explicit decision for 2c (in memory for now), not something hidden
inside the HTTP code.

## Alternatives considered

- **Spring `RestClient`** (what I originally planned). Rejected for this part: its strength is
  mapping JSON to objects, which feeds don't need. What's needed is raw bytes, status, headers,
  a total deadline and a streaming size limit, and the JDK `HttpClient` gives direct control over
  all of those without a Spring web dependency. `RestClient` is still a good fit for the JSON APIs
  in Phase 4 (Hacker News, GitHub).
- **Only a per-request timeout** (`HttpRequest.timeout`). Rejected, and proven wrong, see below.
- **Exceptions for failures.** Rejected for the same reason as in 002: at the network boundary,
  failure is normal. A result type makes every case explicit and countable.
- **`BodyHandlers.ofByteArray()`.** Rejected: it buffers whatever the server sends, however large.

## What happened

- Two tests failed at first, but the bug was in the test: the JDK test server normalizes header
  names (`User-Agent` arrives as `User-agent`). HTTP header names are case-insensitive, so the test
  now compares them that way.
- **The per-request timeout really doesn't protect you.** To check that the "dripping body" test
  proves something, I temporarily replaced the deadline with a plain blocking `send` using only the
  500 ms request timeout. The server sent headers immediately, then one byte every 100 ms. The
  fetch took the full **2.03 s** and reported success. The request timeout only covers waiting for
  headers; once they arrive, a slow server can hold the caller for as long as it likes. With the
  total deadline, the same test fails fast with `TIMEOUT`.

## Measurements

Test-level only: the deadline cuts a 2 s dripping response off at the configured 500 ms. 42 tests
pass (10 for the fetcher).

## Consequences / open questions

- `429 Too Many Requests` and `Retry-After` are treated like any other HTTP error. Revisit if a real
  feed rate-limits us in 2d.
- No gzip: we don't send `Accept-Encoding`, so servers send uncompressed bodies. Fine at a handful
  of feeds; a bandwidth optimization to measure later.
- The client tries to upgrade plain-http connections to HTTP/2 (`Upgrade: h2c`). Harmless so far,
  but worth remembering if an old server misbehaves.
- The limits (deadline, max size) are constructor arguments. 2c will make them configuration.
- Next: 2c, the poller that ties fetching, parsing and mapping together on a schedule.
