---
title: "Mart Models Must Re-alias Snake_case to Quoted Names After Staging Refactor"
category: refactoring
tags: [dbt, marts, power-bi, quoted-aliases, snake-case, staging-refactor, select-star]
module: operations/marts/production
symptoms:
  - Power BI reports show snake_case column headers instead of friendly names
  - Mart model uses select * after upstream intermediate switched to snake_case
  - Quoted alias columns (e.g., "Current Facility ID") disappear from mart output
date_solved: 2026-02-12
---

# Mart Models Must Re-alias Snake_case to Quoted Names After Staging Refactor

## Problem

When refactoring ProdView staging models from Pattern A (quoted descriptive aliases like `"Current Facility ID"`) to Pattern C (snake_case like `current_facility_id`), downstream mart models that relied on `select *` to pass through quoted aliases silently lost their Power BI-friendly column names.

### Example: fct_eng_tank_inventories

Before the refactor, the chain was:
```
stg (quoted) → int (quoted passthrough) → mart (select * → quoted output)
```

After the staging refactor:
```
stg (snake_case) → int (snake_case) → mart (select * → snake_case output)  ← BROKEN for Power BI
```

The mart still compiled and ran — no SQL error — but the output columns changed from `"Current Facility ID"` to `CURRENT_FACILITY_ID`, breaking Power BI reports.

## Root Cause

The refactoring pattern correctly converts staging and intermediate models to snake_case. But mart models that used `select *` were implicitly depending on upstream quoted aliases. When those aliases disappeared, the mart silently changed its output contract.

This also violates the project convention: **no `select *` in marts** (explicit column lists required).

## Solution

Mart models consuming snake_case intermediates must explicitly map to quoted aliases:

```sql
-- fct_eng_tank_inventories.sql (after fix)
select
    tank_record_id as "Tank Record ID",
    tank_id as "Tank ID",
    tank_date as "Date",
    opening_total_volume_bbl as "Opening Total Volume BBL",
    closing_total_volume_bbl as "Closing Total Volume BBL",
    current_facility_id as "Current Facility ID",
    unit_id as "Unit ID",
    unit_type as "Unit Type"
from {{ ref('int_prodview__tank_volumes') }}
where
    tank_date > last_day(dateadd(year, -3, current_date()), year)
    and current_facility_id is not null
```

### The correct layering pattern

| Layer | Column style | Example |
|-------|-------------|---------|
| Staging | snake_case | `current_facility_id` |
| Intermediate | snake_case | `current_facility_id` |
| Mart (Power BI) | snake_case `as "Quoted Alias"` | `current_facility_id as "Current Facility ID"` |

The mart is the **only** layer that should produce quoted aliases, and only when the downstream consumer (Power BI) requires them.

## Checklist for Staging Refactors

When converting a staging model to snake_case:

1. Trace all downstream models: `get_lineage_dev(unique_id="model.*.stg_name", types=["Model"])`
2. For each downstream **intermediate**: update column references to snake_case
3. For each downstream **mart** with Power BI consumers:
   - Replace `select *` with explicit column list
   - Map each snake_case column to its original quoted alias
   - Verify the `where` clause references snake_case (not quoted) column names
4. Check `git diff` to confirm no mart lost its quoted aliases

## Models with Known Power BI Quoted Aliases

These production mart models output quoted aliases for Power BI and must be preserved during refactors:

- `fct_eng_tank_inventories` — fixed in this PR
- `fct_eng_completion_downtimes` — fixed in prior PR (commit `8f97962`)
- `fct_eng_well_header` — quoted aliases preserved via `int_fct_well_header`
- `fct_eng_targets` — quoted aliases preserved via `int_prodview__production_targets`
- `fct_eng_volumes` — quoted aliases preserved via `int_prodview__production_volumes`

## Prevention

1. **Never use `select *` in marts** — always explicit column lists
2. **Before merging a staging refactor**, grep marts for `select *` on any changed intermediate
3. **Add a PR checklist item**: "Verified downstream mart quoted aliases are preserved"

## Related

- Fix commit: `3ff99a5` (fct_eng_tank_inventories)
- Prior fix: `8f97962` (fct_eng_completion_downtimes)
- Staging refactor pattern: `docs/solutions/refactoring/prodview-staging-5-cte-pattern.md`
