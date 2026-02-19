---
title: "ProdView Enterprise Mart — Remaining Sprints (8–10)"
date: 2026-02-19
author: Rob Stover
status: Planned — see Linear project FOR-308 through FOR-313
linear_project: https://linear.app/formentera-ops/project/prodview-enterprise-mart-remaining-ontology-eb8a33fdbb47
---

# ProdView Enterprise Mart — Remaining Sprints Plan

## Context

Sprints 3–7 completed the core ProdView enterprise mart galaxy schema. This document
covers the remaining ProdView entity model domains not yet represented in the mart layer.

### What Was Built (Sprints 3–7)

| Sprint | Model | Source Entity | Rows | Notes |
|--------|-------|--------------|------|-------|
| 3 | `fct_well_production_daily` | pvUnitAllocMonthDay | 23M | Incremental, watermark = `_fivetran_synced` |
| 3 (pre) | `fct_well_production_monthly` | pvUnitAllocMonth | 762K | With LOS financial join |
| 4 | `fct_completion_downtime` | pvUnitCompDownTm | 148K | Islands+gaps algorithm |
| 4 | `plat_well__performance_scorecard` | — | 9,604 | Platinum OBT, expanded w/ Sprint 4+ |
| 5 | `fct_well_test` | pvUnitCompTest | 84K | Carries change-from-prior metrics |
| 5 | `fct_production_target_daily` | pvUnitCompTarget/Day | 2.3M | 4-hop EID chain |
| 6 | `fct_completion_parameters_daily` | pvUnitCompParam | 8.8M | Pressures, temps, plunger ops |
| 6 | `fct_tank_inventory_daily` | pvUnitTankMonthdayCalc | 7.7M | 70.4% EID resolved (pvfac tanks) |
| 7 | `fct_facility_monthly` | pvFacilityMonthCalc | 55K | No EID (separate root entity) |

**EID resolution pattern used across Sprints 3–6 (completion-child entities):**
```
entity.id_rec_parent (completion_id)
  → stg_prodview__completions.id_rec
  → stg_prodview__completions.id_rec_parent (unit_id)
  → well_360.prodview_unit_id   (primary)
  OR stg_prodview__completions.api_10 → well_360.api_10   (fallback)
```

---

## Remaining Gap Analysis

### Sprint 8 — High Priority (FOR-308, FOR-309)

#### FOR-308: `fct_completion_status_history` (pvUnitCompStatus)

**What:** Effective-dated status change events per ProdView completion.
**Why:** Tracks well lifecycle transitions (drilling → producing → shut-in → returned).
Complements `fct_completion_downtime` for root cause analysis and surveillance workflows.

- **Source:** `stg_prodview__status` (PVT_PVUNITCOMPSTATUS)
- **EID chain:** Same 3-hop as completion downtimes
- **Grain:** One row per status record
- **Materialization:** `table` (status changes are infrequent)
- **Pattern:** `models/operations/marts/well_360/fct_completion_downtime.sql`

#### FOR-309: `stg_prodview__facilities` staging model

**What:** Register `PVT_PVFACILITY` as a dbt source and build a 5-CTE staging model.
**Why:** Blocking dependency for Sprint 9a (EID resolution on `fct_facility_monthly`).
The facility EID lives in `pvFacility.IDPa` — not accessible without staging the header table.

- **Source:** `PVT_PVFACILITY` (add to `facilities/_src_prodview.yml`)
- **Key column:** `IDPa` = facility EID
- **Pattern:** `models/operations/staging/prodview/completions/stg_prodview__completions.sql`
- **Validate:** `python scripts/validate_staging.py`

---

### Sprint 9 — Medium Priority (FOR-310, FOR-311)

#### FOR-310: Add EID resolution to `fct_facility_monthly` (blocked by FOR-309)

**What:** Join `stg_prodview__facilities` into `fct_facility_monthly` to populate
a `facility_eid` column.

**Key open question:** `pvFacility.IDPa` is documented as the "EID" for facilities —
but verify whether this maps to `well_360.eid` (which is well-level) or represents
a facility-level identifier. Facilities are multi-well aggregates; the EID
may refer to a battery/lease entity not in well_360.

#### FOR-311: `fct_artificial_lift_daily` (pvUnitCompPump hierarchy)

**What:** Daily operational readings for plunger lift and rod pump installations.

- **Sources:** `stg_prodview__plunger_lift_readings`, `stg_prodview__rod_pump_entries`
- **EID chain:** lift_entry → lift_header → completion → unit → well_360
  (may be 4-hop — validate hierarchy before building)
- **Note:** Plunger readings (cycles, arrivals, travel time) already appear as
  user-defined fields in `fct_completion_parameters_daily`, but dedicated entry
  tables are more complete. Investigate overlap before building.
- **Design question:** Single `fct_artificial_lift_daily` with `lift_type`
  discriminator (UNION), or separate facts per lift type?

---

### Sprint 10 — Lower Priority (FOR-312, FOR-313)

#### FOR-312: Meter readings mart (pvUnitMeter hierarchy)

**What:** Daily raw measurement data from ProdView's 5-type meter hierarchy,
upstream of the allocation engine.

- **Sources (all staged):**
  - `stg_prodview__liquid_meters` + `stg_prodview__liquid_meter_readings`
  - `stg_prodview__gas_meters` + `stg_prodview__gas_meter_readings`
  - `stg_prodview__gas_pd_meters` + `stg_prodview__gas_pd_meter_readings`
  - `stg_prodview__other_measurement_points` + entries
- **EID chain:** 2-hop — meter_entry.id_rec_parent → meter_header.id_rec_parent
  (unit_id) → well_360.prodview_unit_id. No completion hop (meters belong to pvUnit).
- **Volume:** Likely high. Check row counts before choosing table vs incremental.
- **Design decision:** Single UNION fact (`fct_meter_readings_daily` + `meter_type`
  discriminator) vs separate per-type facts. UNION preferred for BI simplicity;
  type-specific details in extension columns or separate tables.

#### FOR-313: Distribution chain facts (pvUnitDistrib)

**What:** Product distribution downstream of allocation.

- **Sources:** `stg_prodview__daily_dispositions`, `stg_prodview__monthly_dispositions`
- **EID chain:** 2-hop — distribution.id_rec_parent → unit → well_360
- **Key caveat:** Disposition columns already exist on `fct_well_production_daily`
  (from the allocation table). Investigate overlap before building. If pvUnitDistrib
  adds purchaser/route/contract context not in the allocation rows, build it; otherwise
  close as redundant.

---

## EID Resolution Patterns Reference

| Hop count | Used by | Chain |
|-----------|---------|-------|
| 2-hop | meters, distribution | entry → unit → well_360 |
| 3-hop | downtime, tests, params, status | entity → completion → unit → well_360 |
| 4-hop | targets, artificial lift | entity → parent → completion → unit → well_360 |
| Separate root | facility | pvFacility.IDPa (not via pvUnit chain) |

---

## Key Gotchas for Future Sprints

1. **`pvFacility` is a separate root entity** — do NOT use the standard pvUnit → child
   join pattern. `pvFacility.IDPa` is the EID but requires `stg_prodview__facilities` first.

2. **Meter EID is 2-hop, not 3-hop** — meter headers (`pvUnitMeterLiquid` etc.) have
   `id_rec_parent` → pvUnit directly (not via pvUnitComp). Skip the completion hop.

3. **Artificial lift is 4-hop minimum** — entries → lift header → completion → unit.
   Validate the actual hierarchy before coding; the intermediate lift header staging
   models may carry the completion ID directly.

4. **`pvFacilityUnit` is a bridge table** (many-to-many: facility ↔ unit). If building
   a facility dimension, use `pvFacilityUnit.IDRecUnit` (not `idrecparent`) to resolve
   which units belong to a facility. Filter `DtTmEnd IS NULL` for current membership.

5. **Source table validation** — always query `information_schema.tables` before
   building any new model. Context docs describe what ProdView *can* expose; Fivetran
   may not have synced every table. See MEMORY.md "Source Table Validation (Critical)".
