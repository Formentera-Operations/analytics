---
title: "pvFacility Is a Separate Root Entity — Do Not Apply pvUnit EID Resolution Chain"
category: logic-errors
tags: [prodview, facility, pvfacility, eid-resolution, root-entity, hierarchy, fct-facility-monthly]
module: operations/marts/well_360
symptoms:
  - 0% EID resolution rate when applying standard completion-child chain to facility records
  - All facility monthly rows have is_eid_unresolved = true despite facilities existing in well_360
  - JOIN produces no matches when using id_rec_parent → completions → units → well_360 for pvFacility
date_solved: 2026-02-19
---

# pvFacility Is a Separate Root Entity — Do Not Apply pvUnit EID Resolution Chain

## Problem

When building `fct_facility_monthly`, the natural instinct was to resolve EIDs using the same chain applied to all other ProdView mart models:

```sql
-- WRONG — applying completion-child chain to a facility entity
left join completions c on f.id_rec_parent = c.completion_id_rec
left join well_dim_primary w1 on c.unit_id_rec = w1.pvunit_id_rec
```

This produces zero EID matches.

## Root Cause

ProdView has two separate entity hierarchies:

```
pvUnit hierarchy:
  pvUnit → pvUnitComp (completion) → pvUnitCompDownTm, pvUnitCompTest, pvUnitCompParam, etc.

pvFacility hierarchy (SEPARATE ROOT):
  pvFacility → pvFacilityMonthCalc, pvFacilityMonthDayCalc, pvFacilityUnit (bridge), etc.
```

`pvFacility` is NOT a child of `pvUnit`. It is an independent root entity. Its `id_rec_parent` in `pvFacilityMonthCalc` references `pvFacility.idrec` directly — it has nothing to do with completions or units.

The facility EID (`pvFacility.IDPa`) must be accessed from the `pvFacility` header table itself, not derived through any pvUnit chain.

**Additional nuance:** A facility is a multi-well aggregate (battery, tank battery, lease group). Its EID may be a facility-level identifier that does NOT map 1:1 to `well_360.eid` (which is well-level). Verify what `pvFacility.IDPa` contains before writing the join.

## Solution (current state — Sprint 7)

`fct_facility_monthly` carries `id_rec_parent as id_rec_facility` as the natural grain key. No EID resolution is applied. The model header documents the deferral:

```sql
{# EID RESOLUTION:
   Not applied in this model. pvFacility.IDPa carries the facility EID but
   PVT_PVFACILITY is not yet registered as a dbt source and no
   stg_prodview__facilities staging model exists. Use `id_rec_facility` to
   join to a future facility dimension when available.
#}
```

## Full resolution path (future Sprint 9a, blocked by Sprint 8b)

When `stg_prodview__facilities` is built (FOR-309):

```sql
facilities as (
    select
        id_rec as facility_id_rec,
        eid as facility_eid  -- from pvFacility.IDPa
    from {{ ref('stg_prodview__facilities') }}
),

-- In the joined CTE:
left join facilities fac on v.id_rec_parent = fac.facility_id_rec
-- facility_eid populated from fac.facility_eid
```

## pvFacilityUnit bridge (for facility membership queries)

If you need to know which wells belong to a facility, use the `pvFacilityUnit` bridge table — NOT `idrecparent`:

```sql
-- pvFacilityUnit.IDRecUnit is the FK to pvUnit/pvUnitComp (NOT idrecparent)
-- Filter DtTmEnd IS NULL for current membership
select facilityunit.idrecunit as unit_id
from pvfacilityunit
where facilityunit.idrecparent = [facility_id]
and coalesce(facilityunit.dttmlend, current_date) >= current_date
```

## Prevention

1. **Before writing any EID chain for a new fact, look up the entity in `context/sources/prodview/domains/`** to confirm whether it's a pvUnit child or pvFacility child.
2. **pvFacility domain YAML** (`context/sources/prodview/domains/facilities.yaml`) explicitly states: "pvFacility is a SEPARATE ROOT ENTITY — NOT a pvUnit child. Do NOT apply the standard pvUnit → child (idrecparent) join pattern here."
3. **Zero match rate is the diagnostic signal** — if EID resolution returns 0% matches, the entity is likely a separate root, not a completion or unit child.

## Related

- `docs/solutions/logic-errors/prodview-eid-resolution-hop-patterns.md` — full hop count reference
- `models/operations/marts/well_360/fct_facility_monthly.sql`
- `context/sources/prodview/domains/facilities.yaml`
- FOR-309 (stg_prodview__facilities staging model)
- FOR-310 (EID resolution on fct_facility_monthly)
