---
title: "ProdView Surrogate Key Collision When Using EID-Coalesce at Unit-Level Grain"
category: logic-errors
tags: [prodview, surrogate-key, grain, eid, id-rec-unit, unique-test, collision]
module: operations/marts/well_360
symptoms:
  - unique test on surrogate key fails with 100,000+ duplicates after initial build
  - dbt build completes successfully but unique test reports "FAIL 106330 results"
  - Multiple rows for the same (eid, allocation_date) in a fact that should be unit-grain
date_solved: 2026-02-19
---

# ProdView Surrogate Key Collision When Using EID-Coalesce at Unit-Level Grain

## Problem

`fct_well_production_daily` initially used a surrogate key modeled after `fct_well_production_monthly`:

```sql
{{ dbt_utils.generate_surrogate_key([
    "case when eid is not null then 'eid' else 'unit' end",
    'coalesce(eid, id_rec_unit)',
    'allocation_date'
]) }} as well_production_daily_sk
```

The unique test failed with 106,330 duplicates.

## Root Cause

The monthly fact aggregates multiple ProdView units to EID grain — one row per (eid, month) after `GROUP BY`. When multiple units share the same EID, they are collapsed into a single row. The EID-coalesce SK is safe in that context.

The daily fact does NOT aggregate — it keeps one row per source unit per day. When multiple `id_rec_unit` values map to the same EID, they produce identical SK values for the same date:

```
Unit A: id_rec_unit="abc", eid="123456", date=2025-01-01
        → SK = MD5('eid', '123456', '2025-01-01') ← DUPLICATE
Unit B: id_rec_unit="xyz", eid="123456", date=2025-01-01
        → SK = MD5('eid', '123456', '2025-01-01') ← DUPLICATE
```

Both units resolve to the same EID (the same well in well_360). At daily grain with no aggregation, both rows exist in the fact. The SK treats them as the same row.

## Solution

For unit-level grain facts, the SK must use `id_rec_unit` directly — not the resolved EID:

```sql
-- CORRECT: unit-level grain → SK based on unit id
{{ dbt_utils.generate_surrogate_key(['id_rec_unit', 'allocation_date']) }}
    as well_production_daily_sk
```

The EID-coalesce namespace pattern (`'eid'` prefix) was only needed to prevent collision between EID values and id_rec_unit values within the same monthly fact row. At unit grain, every row has its own `id_rec_unit`, so this distinction is unnecessary.

## Decision rule

| Grain | Surrogate key pattern |
|-------|--------------------|
| **EID-aggregated** (monthly, scorecard) | `MD5('eid' namespace, coalesce(eid, id_rec_unit), date)` |
| **Unit-level** (daily, not aggregated) | `MD5(id_rec_unit, date)` |
| **Source record** (downtime, tests, etc.) | `MD5(id_rec)` — each source record is already unique |

## Prevention

1. **Determine grain before writing the SK.** If the fact aggregates to EID grain (GROUP BY eid), the EID-coalesce pattern is appropriate. If the fact keeps source-level rows (no aggregation), use the source ID.
2. **Always run `dbt build` (not just `dbt compile`) on initial build.** Compile succeeds even with a wrong SK; the unique test failure only appears at build time when data is queried.
3. **When copying SK patterns from another fact, verify the grain matches.** The monthly fact's SK is NOT safe to copy into a daily fact without review.

## Related

- `docs/solutions/ingestion/prodview-daily-allocation-fivetran-dedup.md` — related unit-date deduplication issue
- `models/operations/marts/well_360/fct_well_production_daily.sql` — corrected implementation
- `models/operations/marts/well_360/fct_well_production_monthly.sql` — EID-aggregate grain where coalesce SK is correct
