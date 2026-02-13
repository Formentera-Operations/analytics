---
title: "Refactoring ProdView Staging Models to 5-CTE Pattern"
category: refactoring
tags: [dbt, prodview, staging, 5-cte, unit-conversions, join-semantics, fivetran]
module: operations/staging/prodview
symptoms:
  - Legacy staging models missing dedup, filtered, enhanced CTEs
  - Inline magic numbers for unit conversions instead of centralized macros
  - Inconsistent column naming (camelCase aliases vs snake_case)
  - Incorrect join semantics in downstream intermediate models
date_solved: 2026-02-12
---

# Refactoring ProdView Staging Models to 5-CTE Pattern

## Problem

Four ProdView staging models and one intermediate model used a legacy pattern that predated the project's 5-CTE staging convention. Issues included:

1. **Missing dedup:** No `qualify row_number()` in source CTE — Fivetran duplicates could leak through
2. **Inline unit conversions:** Magic numbers like `/ 0.158987294928` scattered across SELECT lists instead of using `pv_cbm_to_bbl()` macros
3. **Incorrect soft-delete filtering:** `stg_prodview__tanks` filtered `_fivetran_deleted` in the source CTE (wrong layer) instead of the filtered CTE
4. **Wrong join pattern:** `int_prodview__tank_volumes` used LEFT + RIGHT JOIN + WHERE NULL filter — functionally equivalent to INNER JOINs but confusing and fragile

### Models affected

| Model | Type | Issue |
|-------|------|-------|
| `stg_prodview__tanks` | staging | No 5-CTE pattern, no dedup, filter in wrong CTE |
| `stg_prodview__tank_daily_volumes` | staging | No 5-CTE pattern, no dedup, inline conversions |
| `stg_prodview__rod_pump_configs` | staging | No dedup, inline conversion |
| `stg_prodview__rod_pump_entries` | staging | No dedup, inline conversions |
| `int_prodview__tank_volumes` | intermediate | Confusing LEFT+RIGHT JOIN semantics, old column names |

## Investigation Steps

### 1. Identify column renames that break downstream

Before renaming any staging column, grep for all `ref()` consumers:

```bash
grep -r "ref('stg_prodview__tanks')" models/ --include="*.sql"
grep -r "ref('stg_prodview__tank_daily_volumes')" models/ --include="*.sql"
```

Only `int_prodview__tank_volumes` consumed both models. No other downstream refs existed.

### 2. Map old column names to new

Key renames that required downstream updates:

| Model | Old Column | New Column | Snowflake Source |
|-------|-----------|------------|------------------|
| `stg_prodview__tanks` | `tank_id` | `id_rec` | `IDREC` |
| `stg_prodview__tanks` | `unit_id` | `id_rec_parent` | `IDRECPARENT` |
| `stg_prodview__tank_daily_volumes` | `tank_record_id` | `id_rec` | `IDREC` |
| `stg_prodview__tank_daily_volumes` | `is_deleted` | `_fivetran_deleted` | `_FIVETRAN_DELETED` |

The convention: ProdView `IDREC` maps to `id_rec`, `IDRECPARENT` maps to `id_rec_parent`, `IDFLOWNET` maps to `id_flownet`. These are consistent across all ProdView staging models.

### 3. Verify unit conversion macro equivalence

Each inline factor was matched against the centralized macros in `macros/prodview_helpers/prodview_unit_conversions.sql`:

| Inline Factor | Macro | Domain |
|--------------|-------|--------|
| `/ 0.158987294928` | `pv_cbm_to_bbl()` | Volume: m3 to BBL |
| `/ 28.316846592` | `pv_cbm_to_mcf()` | Volume: m3 to MCF |
| `/ 0.01` | `pv_decimal_to_pct()` | Percentage: decimal to % |
| `/ 0.0254` | `pv_meters_to_inches()` | Length: m to IN |
| `/ 0.1589873` | `pv_cbm_to_bbl_per_day()` | Rate: m3/day to BBL/DAY |

All factors are mathematically identical. The macros include Jinja comments documenting the Peloton source of each factor.

### 4. Validate join semantics change

The old intermediate join pattern:

```sql
from tankvol v
     left join tanks t on v.tank_id = t.tank_id
     right join units u on t.unit_id = u."Unit Record ID"
     where not "Unit ID" is null
```

Analysis: The LEFT JOIN preserves all tank volumes, then the RIGHT JOIN preserves all units. But the WHERE clause (`not "Unit ID" is null`) eliminates any row where the tank's unit_id was NULL — which filters out both unmatched tank volumes AND orphan units. Net result: only rows where all three tables match.

Replaced with equivalent INNER JOINs:

```sql
from tankvol as v
inner join tanks as t
    on v.tank_id = t.id_rec
inner join units as u
    on t.id_rec_parent = u."Unit Record ID"
```

Row count confirmed identical at 2,894,939.

## Solution

### The 5-CTE staging pattern applied

Each staging model was restructured to follow the project's 5-CTE convention:

1. **source** — Raw pull + Fivetran dedup (`qualify row_number() over (partition by idrec order by _fivetran_synced desc)`)
2. **renamed** — Column renaming, type casting (`trim(col)::varchar`), trimming. No filtering, no logic.
3. **filtered** — Remove soft deletes (`coalesce(_fivetran_deleted, false) = false`) and null PKs (`id_rec is not null`)
4. **enhanced** — Surrogate key, computed flags (e.g., `is_active`), `_loaded_at` timestamp
5. **final** — Explicit column list grouped by logical category

### Key patterns used

**Fivetran dedup in source CTE:**
```sql
source as (
    select * from {{ source('prodview', 'PVT_PVUNITTANK') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),
```

**Unit conversions via macros (in renamed CTE):**
```sql
{{ pv_cbm_to_bbl('volcapacity') }}::float as tank_capacity_bbl,
{{ pv_decimal_to_pct('pctfullcalc') }}::float as capacity_percent_full,
```

**Defensive soft-delete filter (in filtered CTE):**
```sql
filtered as (
    select *
    from renamed
    where
        coalesce(_fivetran_deleted, false) = false
        and id_rec is not null
),
```

**Metadata columns prefixed with underscore:**
```sql
_fivetran_deleted::boolean as _fivetran_deleted,
_fivetran_synced::timestamp_tz as _fivetran_synced,
-- In enhanced:
current_timestamp() as _loaded_at
```

### Downstream intermediate update

Updated `int_prodview__tank_volumes` to use the new column names and INNER JOIN semantics. Also updated the filter from `where is_deleted = false` to `where coalesce(_fivetran_deleted, false) = false` to match the renamed column.

## Gotchas

### 1. Column renames cascade to quoted aliases in intermediate

`int_prodview__tank_volumes` outputs quoted aliases (e.g., `"Tank ID"`, `"Unit ID"`) for a downstream application layer. When staging columns were renamed, the SELECT expressions needed updating but the quoted output aliases stayed the same — preserving the downstream contract.

### 2. `stg_prodview__units` is still legacy

The units model still uses legacy quoted aliases (`"Unit Record ID"`, `"Unit Type"`) and has no 5-CTE pattern. The intermediate model references these quoted names directly. When units gets refactored in a future PR, `int_prodview__tank_volumes` will need another update.

### 3. Dedup was silently missing

None of the four models had Fivetran dedup before this refactor. In practice, Fivetran deduplication at the connector level likely prevented issues, but the `qualify` pattern is a defensive best practice that costs nothing and prevents subtle row multiplication bugs.

### 4. `is_active` flag pattern

The `is_active` flag derives from date nullability:

```sql
-- Correct: simple boolean expression
stop_using_tank is null as is_active

-- Unnecessary (IS NULL never returns NULL):
coalesce(stop_using_tank is null, false) as is_active
```

The `coalesce` wrapper is harmless but redundant — `IS NULL` always returns TRUE or FALSE, never NULL.

## Prevention

1. **Use the 5-CTE pattern for all new staging models.** The template in `CLAUDE.md` is the source of truth.
2. **Always use `pv_*` macros for ProdView unit conversions.** Never inline the divisor factors — they're hard to audit and easy to typo.
3. **Grep for downstream consumers before renaming columns.** Even one missed ref causes a build failure.
4. **Prefer INNER JOIN when the intent is INNER JOIN.** LEFT/RIGHT + WHERE NULL filters obscure intent and confuse reviewers.
5. **Add Fivetran dedup to every staging model at creation time.** The `qualify` pattern is zero-cost insurance.

## Related

- PR branch: `refactor/prodview-staging-downstream-easy`
- Commits: `7c319b3` (staging refactor), `8a48877` (intermediate join fix)
- Unit conversion macros: `macros/prodview_helpers/prodview_unit_conversions.sql`
- Prior solution: `docs/solutions/build-errors/dbt-model-deletion-rename-procedure.md`
