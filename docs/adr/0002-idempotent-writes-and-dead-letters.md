# ADR 0002: Idempotent-by-construction writes, bisection and re-planned dead letters

- Status: accepted
- Date: 2026-11-19

## Context

HubSpot gives us at-least-once semantics at best:

- a request can be committed on the server even though the client sees a timeout or a 502;
- batch endpoints reject the **whole** batch (HTTP 400) when one input has an invalid value, and
  do not say which input it was;
- batch update and read answer 207 Multi-Status with per-item errors (for example, an id that no
  longer exists);
- there is no `Idempotency-Key` header and no conditional write (`If-Match`).

## Decision

1. **Every write we issue is idempotent, so retrying is always safe.**
   - Creates go through `batch/upsert` keyed on a *unique* property (`email`, or a custom
     `warehouse_id`). Replaying an upsert cannot create a duplicate.
   - Updates send absolute values for the changed fields only, never deltas.
   - Archive and associate are set operations.
   - Every input carries a deterministic `objectWriteTraceId` (a hash of object, key, writes and
     base) for tracing. Each batch also sends an `Idempotency-Key` header. Real HubSpot ignores
     that header. The fake server can honour it, but that is **off by default**, and the
     lost-response tests run with it off to show that correctness does not depend on it.
2. **Retry policy.** 429 triggers a global cool-down that honours `Retry-After`. 5xx and
   transport errors get exponential backoff with jitter.
3. **Partial failures.**
   - On a 400/409/422 for a multi-item batch, split the batch in half and recurse until the
     poison inputs are isolated. One bad record in 100 costs about 14 extra calls instead of
     dropping 99 good ones.
   - 207 errors are mapped back to their inputs by id or trace id.
   - `OBJECT_NOT_FOUND` on update means a rep deleted the record. We record a tombstone and do not
     resurrect it.
4. **Durability.** Base state is committed after *each* batch. Watermarks are committed only after
   the whole run succeeds. If the process crashes mid-run, the next run re-plans: work that
   already landed shows up as "in sync", and the rest is redone.
5. **Dead letters keep keys, not payloads.** A permanent failure is stored as `(object, key, op,
   error, attempts)`. Open keys are retried automatically for `max_auto_retries` runs, then they
   are parked. `crm-sync replay` **re-plans** those keys against fresh warehouse and CRM state
   and does not resend the old payload. Resending the stored request would push stale values once
   someone has fixed the data.

## Consequences

- The tests cover: a crash on the 2nd upsert batch, "applied but the response was lost" (a 502
  after commit), random 429/5xx, and poison rows. All of them converge with zero duplicates.
- Bisection adds calls in proportion to the number of poison rows (measured in the benchmark's
  `faults` scenario).
- Without CAS on the HubSpot side there is still a small window for lost updates. ADR 0003 covers
  how we narrow it.
