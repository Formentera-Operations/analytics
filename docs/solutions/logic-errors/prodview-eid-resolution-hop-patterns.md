---
title: "ProdView EID Resolution — Hop Count Patterns and api_10 Fan-Out Prevention"
category: logic-errors
tags: [prodview, eid-resolution, well_360, surrogate-key, fan-out, api-10, completions-bridge]
module: operations/marts/well_360
symptoms:
  - Production volumes doubled for wells sharing an api_10
  - Fact table row count is higher than expected after joining to well_360
  - Unresolved wells (is_eid_unresolved = true) unexpectedly high despite most wells having EIDs
  - Wrong EID assigned when multiple wells share an api_10 (non-operated well selected over operated)
date_solved: 2026-02-19
---

# ProdView EID Resolution — Hop Count Patterns and api_10 Fan-Out Prevention

## Problem

ProdView mart models must join to `well_360` to resolve an EID (Entity Identifier) — the canonical well key used across all source systems. The join is non-trivial because:

1. The hop count between the source record and the unit ID varies by entity type.
2. The api_10 fallback produces fan-out (duplicate rows) when multiple wells share the same api_10.
3. The wrong EID gets selected if the fallback deduplication doesn't prefer operated wells.

## Root Cause

ProdView's hierarchy places different entity types at different levels:

- **pvUnitComp** (completions) are children of **pvUnit** (units). Most completion-child entities (downtime, tests, params, status) have `id_rec_parent` → completion, which then needs one more hop to the unit via `completions.id_rec_parent`.
- **pvUnitMeter** entries are direct children of pvUnit — no completion hop needed.
- **pvFacility** is a completely separate root entity — it does NOT live in the pvUnit hierarchy at all.

The api_10 fallback must deduplicate because multiple ProdView `pvUnitComp` records can share the same api_10 (e.g., an operated completion and a non-operated sibling well with identical API). Without deduplication, the LEFT JOIN fans out and doubles volume rows.

## Solution

### Hop count reference

| Entity type | Hop count | Chain |
|-------------|-----------|-------|
| Meter entries, distribution | 2 | entry → pvUnit → well_360 |
| Downtime, tests, params, status | 3 | entity → completion → pvUnit → well_360 |
| Target daily, artificial lift entries | 4 | entity → parent record → completion → pvUnit → well_360 |
| pvFacility | Separate root | pvFacility.IDPa → well_360 (requires stg_prodview__facilities) |

### Standard 3-hop EID resolution CTEs (used across all completion-child facts)

```sql
-- BRIDGE: stg_prodview__completions carries BOTH id_rec_parent (unit_id)
-- AND api_10 directly — no separate stg_prodview__units join needed for 3-hop
completions as (
    select
        id_rec as completion_id_rec,
        id_rec_parent as unit_id_rec,   -- = pvUnit.idrec
        api_10 as completion_api_10
    from {{ ref('stg_prodview__completions') }}
),

well_dim_primary as (
    -- Primary: ProdView unit ID → well_360.prodview_unit_id
    select prodview_unit_id as pvunit_id_rec, eid
    from {{ ref('well_360') }}
    where prodview_unit_id is not null
),

well_dim_fallback as (
    -- Fallback: api_10 → well_360.api_10
    -- QUALIFY is REQUIRED to prevent fan-out when multiple wells share api_10
    -- Prefer operated wells; break ties by lowest eid
    select api_10 as pvunit_api_10, eid
    from {{ ref('well_360') }}
    where api_10 is not null
    qualify row_number() over (
        partition by api_10
        order by case when is_operated then 0 else 1 end, eid
    ) = 1
)
```

### Applying EID resolution in the fact CTE

```sql
production_with_eid as (
    select  -- noqa: ST06
        coalesce(w1.eid, w2.eid) as eid,
        coalesce(w1.eid, w2.eid) is null as is_eid_unresolved,
        e.*   -- all other columns
    from entity e
    left join completions c on e.id_rec_parent = c.completion_id_rec
    left join well_dim_primary w1 on c.unit_id_rec = w1.pvunit_id_rec
    left join well_dim_fallback w2 on c.completion_api_10 = w2.pvunit_api_10
)
```

### 2-hop variant (meters, distribution — direct unit child)

For entities whose `id_rec_parent` points to pvUnit directly (no completion hop):

```sql
-- Meter entries join directly to units, then to well_360
-- Need stg_prodview__units for api_10 since there's no completions bridge
units as (
    select id_rec as unit_id_rec, api_10
    from {{ ref('stg_prodview__units') }}
),

joined as (
    select
        coalesce(w1.eid, w2.eid) as eid, ...
    from meter_entries e
    left join units u on e.id_rec_parent = u.unit_id_rec
    left join well_dim_primary w1 on e.id_rec_parent = w1.pvunit_id_rec
    left join well_dim_fallback w2 on u.api_10 = w2.pvunit_api_10
)
```

### Tank variant (stg_prodview__units required for fallback)

Tanks have `id_rec_parent` on the tank header (not the daily volumes). Daily volumes join through tank header → unit:

```sql
tanks as (
    select id_rec as tank_id_rec, id_rec_parent as unit_id_rec, ...
    from {{ ref('stg_prodview__tanks') }}
),
units as (
    select id_rec as unit_id_rec, api_10
    from {{ ref('stg_prodview__units') }}
),
-- tanks don't carry api_10 — must join through units for fallback
joined as (
    left join tanks t on v.tank_id = t.tank_id_rec
    left join units u on t.unit_id_rec = u.unit_id_rec
    left join well_dim_primary w1 on t.unit_id_rec = w1.pvunit_id_rec
    left join well_dim_fallback w2 on u.api_10 = w2.pvunit_api_10
)
```

### Expected resolution rates

| Entity | Expected resolution | Notes |
|--------|--------------------|----|
| Completion-child (downtime, tests, params) | ~99–99.9% | Small unresolved: non-operated, SWD |
| Daily production (pvunitcomp units) | ~99.3% | Historical non-operated wells |
| Tank daily volumes | ~70.4% | 29.6% = pvfac facility/battery tanks (expected) |
| Facility monthly | N/A | pvFacility is separate root; needs stg_prodview__facilities |

Always use `severity: warn` on `not_null_eid` and `relationships` tests — unresolved rows are retained (never dropped).

## Prevention

1. **Always include the QUALIFY dedup** in `well_dim_fallback` — fan-out from shared api_10 is silent (no error, just doubled volumes).
2. **Use `stg_prodview__completions` as the bridge** for all 3-hop entities — it carries both `id_rec_parent` (unit_id) and `api_10`, avoiding a separate units join.
3. **For tank facts**, explicitly join `stg_prodview__units` — tanks don't carry api_10 directly.
4. **Never apply pvUnit EID chain to pvFacility** — facilities are a separate root; see `prodview-pvfacility-separate-root.md`.
5. **Carry `is_eid_unresolved` flag** in every fact — document expected rates in schema YAML.

## Related

- `docs/solutions/logic-errors/prodview-pvfacility-separate-root.md`
- `docs/solutions/logic-errors/prodview-eid-coalesce-sk-collision.md`
- `context/sources/prodview/entity_model.md` — EID Resolution Strategy section
- `models/operations/marts/well_360/fct_completion_downtime.sql` — canonical 3-hop reference implementation
