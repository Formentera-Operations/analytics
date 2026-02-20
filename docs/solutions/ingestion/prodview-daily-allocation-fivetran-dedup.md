---
title: "ProdView Daily Allocations Have Duplicate (id_rec_unit, date) Pairs from Fivetran Re-Ingestion"
category: ingestion
tags: [prodview, fivetran, deduplication, daily-allocations, grain, qualify, unique-test]
module: operations/staging/prodview
symptoms:
  - unique test on fct_well_production_daily surrogate key fails after fixing EID-coalesce SK
  - 23,097,381 total rows vs 23,096,903 distinct (id_rec_unit, allocation_date) pairs — 478 duplicates
  - All duplicate unit-date pairs from recent dates (within last 60–90 days)
  - Each duplicate pair has exactly 2 rows with different id_rec values but identical unit/date
date_solved: 2026-02-19
---

# ProdView Daily Allocations Have Duplicate (id_rec_unit, date) Pairs from Fivetran Re-Ingestion

## Problem

After fixing the surrogate key collision (see `prodview-eid-coalesce-sk-collision.md`), the unique test on `fct_well_production_daily` still failed — but with a much smaller count (106,330 → 478 duplicate unit-date pairs, or ~956 rows).

All 478 duplicate pairs were from recent dates, each had exactly `cnt = 2`, and each pair had different `id_rec` values (different allocation record IDs) for the same `id_rec_unit` + `allocation_date`.

## Root Cause

`stg_prodview__daily_allocations` deduplicates by `idrec` (the allocation record's own ID):

```sql
source as (
    select * from {{ source('prodview', 'PVT_PVUNITALLOCMONTHDAY') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
)
```

This ensures each `idrec` appears only once (keeping the latest Fivetran-synced version). But it does NOT guarantee uniqueness at `(id_rec_unit, allocation_date)` grain.

When ProdView re-allocates a day's production (a common end-of-month operation), it creates a NEW `idrec` for the revised allocation while potentially leaving the old one in the database. Both pass the staging dedup because they have different `idrec` values. Both appear in the staging view. Both land in the fact.

## Solution

Add a second QUALIFY to the `daily_allocations` CTE in the fact model to enforce unit-date grain uniqueness:

```sql
daily_allocations as (
    select
        ...all columns...
    from {{ ref('stg_prodview__daily_allocations') }}
    {% if is_incremental() %}
        where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})
    {% endif %}
    -- Deduplicate to one record per (id_rec_unit, allocation_date).
    -- Staging deduplicates by idrec only; Fivetran re-ingestion of
    -- re-allocated records creates multiple irecs for the same unit-date.
    -- Keep the latest-synced authoritative record.
    qualify row_number() over (
        partition by id_rec_unit, allocation_date
        order by _fivetran_synced desc, id_rec asc
    ) = 1
),
```

`_fivetran_synced desc` keeps the most recently ingested record (the authoritative re-allocation). `id_rec asc` is a deterministic tiebreaker if two records share the same Fivetran sync timestamp.

## Prevention

1. **Pattern:** When a fact sources from a ProdView staging VIEW that deduplicates by `idrec`, and the intended fact grain is finer than `idrec` uniqueness, add a grain-level QUALIFY in the fact's source CTE.
2. **Check uniqueness early:** After the initial `dbt build --full-refresh`, run `dbt show --inline "select count(*) as rows, count(distinct id_rec_unit || '|' || allocation_date) as distinct_pairs from ..."` to catch this before tests run.
3. **Recent-date duplicates are normal:** Duplicates appearing only on recent dates (last 60–90 days) are almost always Fivetran re-ingestion artifacts, not data quality bugs. ProdView routinely revises recent allocations.

## Related

- `docs/solutions/logic-errors/prodview-eid-coalesce-sk-collision.md` — the surrogate key issue discovered first
- `models/operations/marts/well_360/fct_well_production_daily.sql` — corrected implementation
