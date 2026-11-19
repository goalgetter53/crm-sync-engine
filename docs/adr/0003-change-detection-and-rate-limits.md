# ADR 0003: Change detection, revalidation and the rate-limit scheduler

- Status: accepted
- Date: 2026-11-19

## Context

- HubSpot private apps get 100 requests per 10 s (Free/Starter) or 190 (Pro/Enterprise), plus a
  daily cap shared by every app in the account (250k to 1M). The CRM search API is limited to
  5 requests/s per account, returns at most 200 results per page, refuses to page past 10,000
  results, and is *eventually consistent* ("it may take a few moments" for changes to show up).
- Warehouses rarely have reliable change data capture. `updated_at` is common but not always
  bumped, and long transactions commit rows whose `updated_at` is older than rows that are
  already visible.

## Decision

### Warehouse side
- An incremental pull on `updated_at >= watermark - lookback` (default lookback 300 s), followed
  by a **row-hash comparison** against the stored snapshot. Re-reading rows in the overlap costs
  nothing, because rows with an unchanged hash are dropped.
- A cheap key scan (`pk, match column`) on every run finds inserts and deletes. Associations are
  diffed as sets over a similar projection, so a target that shows up later still gets its old
  references linked.
- `--full-refresh` hashes every row. Use it to catch edits that did not bump `updated_at`. A test
  shows that such an edit is missed incrementally and caught by a full refresh.

### CRM side
- Search on `hs_lastmodifieddate >= watermark - search_lag`, sorted ascending, with 200 results
  per page. Before reaching 10,000 results, restart the query from the last timestamp seen and
  dedupe by id.
- The watermark is the time the pull *started*. The lag overlap absorbs indexing delay.
- **Search only discovers candidates.** Values come from `batch/read`, which is consistent.
- **Echo suppression.** Records whose indexed values equal the CRM side of the base (usually our
  own writes coming back) are dropped before any read.

### Revalidation (closing most of the lost-update window)
Immediately before each write batch, re-read the target records (`batch/read`, 1 call per 100
records) and re-run the merge with the fresh CRM values. A rep edit made after planning is merged
under the field's policy instead of being overwritten. Warehouse write-backs use SQL
compare-and-swap (`UPDATE ... WHERE col = expected`). If the CAS fails, the record's stale hash is
kept so the next run re-merges it.

### Scheduler
- A token bucket per limit class (general and search), **plus an exact rolling-window log**,
  because HubSpot's windows roll. A bucket alone allows `capacity + rate × window` requests within
  a single window right after a burst, which earns 429s. A test reproduces this.
- A daily budget that fails fast (`DailyLimitExceeded`) before it burns the account's shared
  quota.
- On a 429, all traffic pauses for `Retry-After` (or exponential backoff when the header is
  missing).
- `X-HubSpot-RateLimit-Remaining` and `-Daily-Remaining` headers drain the local counters, so
  several workers sharing one app converge.

## Consequences

- Guarantees hold under a stated quiescence condition: once edits stop and the search index has
  caught up (within `search_lag_seconds`), one sync converges and the next is a no-op.
- Revalidation costs about 1 extra call per 100 written records. A residual window remains between
  the revalidation read and the write (a few ms on localhost, one round trip on HubSpot). An edit
  to a *different* field in that window is never lost, because we send only the changed fields.
  An edit to the *same* field can be. The tests document this window explicitly.
- Right after a bulk load, the next run pages through our own echoes (252 search calls for 50k
  records in the benchmark). Steady-state no-op runs cost 1 search call per object.
- The incremental warehouse pull re-hashes every row in the lookback window, so a no-op run just
  after a bulk load scans every recently written row.
