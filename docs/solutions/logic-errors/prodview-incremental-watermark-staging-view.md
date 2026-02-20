---
title: "ProdView Incremental Watermark Fails When Sourcing from Staging VIEW (_loaded_at unusable)"
category: logic-errors
tags: [prodview, incremental, dbt, watermark, fivetran-synced, staging-view, loaded-at]
module: operations/marts/well_360
symptoms:
  - Incremental dbt model performs a full table scan on every run despite watermark filter
  - _loaded_at watermark filter passes all rows because staging VIEW recomputes current_timestamp() on every query
  - Initial --full-refresh and incremental runs produce identical row counts
  - Incremental run takes as long as the full refresh
date_solved: 2026-02-19
---

# ProdView Incremental Watermark Fails When Sourcing from Staging VIEW

## Problem

`fct_well_production_daily` is an incremental model that sources from `stg_prodview__daily_allocations`. The intended watermark pattern was:

```sql
{% if is_incremental() %}
where _loaded_at > (select max(_loaded_at) from {{ this }})
{% endif %}
```

This caused the incremental model to behave like a full refresh on every run — filtering no rows at all.

## Root Cause

ProdView staging models are materialized as **VIEWs** (not tables). The `enhanced` CTE in every staging view computes `_loaded_at` as:

```sql
enhanced as (
    select *, current_timestamp() as _loaded_at
    from filtered
)
```

A VIEW doesn't store data — it re-executes this SQL on every query. So when the incremental fact queries:

```sql
where _loaded_at > (select max(_loaded_at) from {{ this }})
```

The right side returns the last run's timestamp (e.g., `2026-02-19 20:30:00`). But the left side (`_loaded_at` from the staging VIEW) returns `current_timestamp()` — always NOW. Every row is always "newer" than the watermark. The filter is always true. Every row is always included.

## Solution

Use `_fivetran_synced` as the incremental watermark instead of `_loaded_at`:

```sql
{% if is_incremental() %}
where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})
{% endif %}
```

`_fivetran_synced` is the actual Fivetran ingestion timestamp — it's stored on the source table and reflects when Fivetran actually wrote the row. It updates only when Fivetran re-syncs a record, making it a stable, reliable watermark.

**Also required:** Carry `_fivetran_synced` in the fact's output columns so the next incremental run can compare against it:

```sql
final as (
    select
        ...all other columns...,
        _fivetran_synced,                    -- carry for watermark on next run
        current_timestamp() as _loaded_at    -- still useful as load timestamp
    from production_with_eid
)
```

## Prevention

1. **Rule:** Never use `_loaded_at` as an incremental watermark when the source is a dbt VIEW (any staging model). `_loaded_at` in staging VIEWs is always `current_timestamp()`.
2. **Check materialization first:** Before writing the watermark filter, verify the upstream model's materialization. `view` → use `_fivetran_synced`. `table` → `_loaded_at` is safe.
3. **Fivetran sources only:** `_fivetran_synced` is available on all Fivetran-synced tables. For Estuary sources, use `_flow_published_at` instead (same concept — the connector's ingestion timestamp).
4. **Verify the filter works:** After initial `--full-refresh`, run a second incremental build immediately. It should process 0 new rows if the watermark is correct.

## Related

- `docs/conventions/incremental.md` — project incremental pattern
- `models/operations/marts/well_360/fct_well_production_daily.sql` — reference implementation
- `docs/reference/source-systems.md` — Fivetran vs Estuary source systems
