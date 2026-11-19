# ADR 0001: Field-level three-way merge with a per-side base

- Status: accepted
- Date: 2026-11-19

## Context

Reverse-ETL tools usually treat the warehouse as the single source of truth and overwrite the CRM
on every run. That is how a sales rep's correction of a phone number, or a lifecycle stage they
moved by hand, disappears overnight. A bi-directional sync has to tell three situations apart for
every field:

1. only the warehouse changed: push it;
2. only the CRM changed: keep it, or pull it back into the warehouse;
3. both changed: a real conflict that needs a declared policy.

A two-way comparison (warehouse vs CRM) cannot separate these, because "they differ" looks the same
in all three cases. You need a *base*: what each side held after the last successful sync.

## Decision

- Store a base **per field and per side**: `(base_wh, base_crm)`, in `sync_base.fields`.
  A single shared base value is not enough. Policies such as `crm_wins` deliberately leave the two
  sides different, and with one base the next run would read that steady state as a fresh
  warehouse change and overwrite the rep.
- Compare in a canonical string space (HubSpot sends every value as a string). Warehouse values go
  through the field's transform first. Fields that write back may only use idempotent normalizer
  chains (`lower(trim(value))`, `e164(value)`), and the same normalizer is applied to the CRM
  value. This is checked when the mapping is validated.
- The resolution table lives in one pure function, `merge_field()`, with no I/O. The planner and
  the executor's revalidation step both call it.
- The policies (`source_wins`, `crm_wins`, `latest_timestamp_wins`, `fill_if_empty`,
  `never_overwrite`) are set per field, together with a direction (`to_crm`, `bidirectional`,
  `to_warehouse`).
- `latest_timestamp_wins` compares *record-level* timestamps: warehouse `updated_at` against
  HubSpot `hs_lastmodifieddate`. Ties go to the warehouse.

## Consequences

- Rep edits are kept by construction wherever the policy says so. Hypothesis checks this on
  random inputs (`tests/test_merge.py`) and across whole sync histories
  (`tests/test_properties.py`).
- Idempotence is a property of the merge itself: once a decision is applied and the base is
  updated, merging again writes nothing. This is tested over every policy and direction.
- State grows with *records × mapped fields*. That is fine at CRM scale (around 10^5–10^6
  records).
- Record-level timestamps are coarse. If a rep edits field A, `latest_timestamp_wins` on field B
  also sees the CRM as "newer". Per-property timestamps (`propertiesWithHistory`) would fix this,
  at the cost of much heavier reads. Listed under Limitations.
- Changing a mapping's policy reinterprets the existing base. That is safe, because the base
  records observed values, not decisions.
