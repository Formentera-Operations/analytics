---
title: "ProdView Enterprise Mart — Full Ontology Analytics Architecture"
date: 2026-02-19
author: Rob Stover
status: Approved — proceed to planning
---

# Brainstorm: ProdView Enterprise Mart — Full Ontology Analytics Architecture

## What We're Building

A complete enterprise mart layer over ProdView that covers every major analytical domain in the entity model: production volumes at daily grain, production efficiency (downtime), well performance testing, operational surveillance (parameters/pressures), targets vs actuals, and eventually tank inventory and facility-level rollups.

The goal is "full ontology analytics" — every business process in ProdView represented as a grain-appropriate fact table at the gold layer, with summary metrics folded into the platinum scorecard.

### Consumer Targets
- **Power BI / Cortex Analyst** — requires denormalized OBTs at platinum for joins-free query time
- **dbt Semantic Layer / MetricFlow** — requires normalized gold-layer facts with clear grain and composable metrics
- **Both equally** — confirmed: build normalized gold + denormalized platinum

---

## Why This Approach

ProdView staging is already complete (~52 models). The analytical gap is entirely at the mart layer. Rather than building all 8 fact tables at once, we build sprint-by-sprint from highest analytical value to lowest, following the galaxy schema pattern already established in `fct_well_production_monthly` + `plat_well__performance_scorecard`.

**Independent DAG principle:** ProdView manages separate source tables for daily vs monthly allocations (`PVT_PVUNITALLOCMONTHDAY` vs `PVT_PVUNITALLOCMONTH`). The daily and monthly production facts are fully independent — no monthly-derives-from-daily relationship required. Each sources from its own ProdView staging model.

---

## Key Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Daily fact source | `stg_prodview__daily_allocations` directly | Carries ~70+ columns at row level including downtime_hours, deferred volumes, WI/NRI, dispositions — no intermediate needed |
| Daily fact column scope | **Wide** — full column set from staging | ~40-50 mart-relevant columns: all volume streams, deferred, WI/NRI, dispositions, operational time, heat content, density, FK bridges. Self-contained for ad-hoc. Platinum handles curation. |
| Daily historical lookback | **Full history** — all available Fivetran data | No artificial cutoff. `--full-refresh` initial load, then incremental. Enables full decline curves and cumulative validation. |
| Daily vs monthly relationship | Independent DAGs | ProdView has separate source tables (`PVUNITALLOCMONTHDAY` vs `PVUNITALLOCMONTH`); no derived dependency. Monthly stays as-is with its LOS join. |
| Daily fact materialization | `incremental` | Will exceed 1M rows historically (2K wells × 365 days × full history) |
| Daily fact cluster | `[eid, allocation_date]` | Two most common query filters |
| Monthly fact after daily exists | Leave as-is | Monthly has its own ProdView source table and LOS financial context that daily won't carry. Both coexist as independent facts. |
| Downtime fact | New ephemeral intermediate in Sprint 4 | Build `fct_completion_downtime` with a NEW intermediate sourced from `stg_prodview__completion_downtimes` directly, bypassing the legacy `int_prodview__completion_downtimes`. Legacy intermediate + `fct_eng_completion_downtimes` continue untouched until Sprint 7 deprecation. |
| Platinum strategy | Expand existing scorecard | Add summary stats from new facts; keep single EID-grain OBT |
| EID resolution | Same two-step COALESCE as monthly | Primary: `unit.id_rec = well_360.prodview_unit_id`; Fallback: `api_10`; `is_eid_unresolved` flag |
| Legacy model protection | Non-breaking | All fct_eng_* models stay running until Sprint 7 deprecation sprint. New models are additive. Never touch a legacy intermediate without auditing its consumers first. |

---

## Sprint Roadmap

### Sprint 3 — Daily Production Fact (highest priority)
**Model:** `fct_well_production_daily`
**Grain:** One row per (EID, allocation_date) for resolved wells; (id_rec_unit, allocation_date) for unresolved
**Source:** `stg_prodview__daily_allocations` (direct, no intermediate)
**Materialization:** `incremental`, `merge` strategy, watermark = `_loaded_at`, cluster = `['eid', 'allocation_date']`
**Key columns (not the full 70+ staging columns):**
- Production: allocated oil/gas/water/NGL/condensate (bbl/mcf), gathered volumes, BOE
- Operational time: `downtime_hours`, `operating_time_hours` (from daily alloc row)
- Deferred: deferred oil/gas/water volumes
- WI/NRI: working interest % and net revenue interest % per product
- Disposition summary: sales, fuel, flare, vent, injected
- FK bridges: `id_rec_downtime`, `id_rec_test`, `id_rec_param`, `id_rec_status` (for future event-level joins)
- EID resolution: `eid`, `is_eid_unresolved`, `id_rec_unit`

**Replaces:** `fct_eng_volumes` functionally (but not removed yet — migration sprint separate)
**Unblocks:** Platinum scorecard last-30-day production metrics, decline curve analysis, GOR/WOR trending

---

### Sprint 4 — Downtime Fact + Scorecard Expansion
**Models:**
1. `fct_completion_downtime` — event-level downtime (per contiguous event)
2. Expand `plat_well__performance_scorecard` with new summary columns

**`fct_completion_downtime` grain:** One row per contiguous downtime event per completion (island-merged)
**Source:** Refactor `int_prodview__completion_downtimes` to add EID resolution, then materialize as a fact table
**Key:** The existing intermediate has islands+gaps logic (LAG + running SUM) — keep that, add EID COALESCE, promote from view to `table`
**Materialization:** `table` (downtime events are low volume)
**Key columns:** `event_start_date`, `event_end_date`, `total_downtime_hours`, `days_down`, `downtime_code_1/2/3`, `is_failure`, `product`, `location`, `eid`, `id_rec_comp`

**Scorecard additions:**
- `lifetime_downtime_hours` — sum from `fct_completion_downtime`
- `downtime_event_count_12m` — rolling 12-month event count
- `last_prod_date` — max allocation_date from `fct_well_production_daily`
- `days_since_last_production` — datediff from last_prod_date to today
- `avg_daily_oil_bbl_30d`, `avg_daily_gas_mcf_30d` — rolling 30-day from daily fact

**Hard dependency:** Sprint 3 must complete before scorecard expansion (needs `fct_well_production_daily`)

---

### Sprint 5 — Well Tests + Production Targets
**Models:**
1. `fct_well_test` — per test event (PI, injection ratio, test volumes)
2. `fct_production_target_daily` — targets vs actuals per completion per day

**Notes:**
- Both intermediates exist (int_prodview__production_targets as a view) but lack EID resolution
- `fct_well_test` grain: one row per production test (not daily) — PI trending over time
- `fct_production_target_daily` enables variance analysis (actual vs. target volumes)
- Both refactor existing intermediates rather than building from scratch

---

### Sprint 6 — Surveillance (Parameters / Tank Inventory)
**Models:**
1. `fct_completion_parameters_daily` — daily pressures, temps, choke per completion
2. `fct_tank_inventory_daily` — daily tank readings and calculated volumes

**Notes:**
- Lower demand than volumes/downtime/tests but needed for full surveillance ontology
- Tank fact replaces `fct_eng_tank_inventories` (legacy) eventually
- Parameters fact is new — no legacy equivalent

---

### Sprint 7 — Facility Level + Deprecation
**Models:**
1. `fct_facility_monthly` — facility-level production/injection balance
2. Deprecate legacy models: `fct_eng_volumes`, `fct_eng_completion_downtimes`, `fct_eng_tank_inventories`, `fct_eng_targets`

---

## Open Questions (Resolved)

All major decisions resolved in the brainstorm dialogue. Remaining open items for Sprint planning:

1. **Platinum timing**: Scorecard expansion depends on Sprint 3 completion. Upgrade in-place (no versioning) — new columns are additive and non-breaking for existing consumers. Schedule as Sprint 4 deliverable once Sprint 3 is merged.

2. **Legacy intermediate audit before Sprint 7**: Before deprecating `int_prodview__completion_downtimes`, audit all downstream consumers (`fct_eng_completion_downtimes` confirmed; check for any others). Create a migration ticket for each.

3. **Daily fact initial load sizing**: `stg_prodview__daily_allocations` full history row count unknown until first build. Expected: 2-5M rows. If initial `--full-refresh` is too slow, consider a one-time backfill strategy with warehouse size upgrade for the initial run.

---

## Files To Create / Modify

**New (Sprint 3):**
- `models/operations/marts/well_360/fct_well_production_daily.sql`
- `models/operations/marts/well_360/_marts_well_360.yml` (update with new model)

**Refactored (Sprint 4):**
- `models/operations/intermediate/production/int_prodview__completion_downtimes.sql` — add EID resolution, remove date cutoff
- `models/operations/platinum/well_360/plat_well__performance_scorecard.sql` — add new summary columns

**New (Sprint 4):**
- `models/operations/marts/well_360/fct_completion_downtime.sql`

---

## Context References

- `docs/conventions/marts.md` — materialization, clustering, naming
- `docs/conventions/incremental.md` — merge strategy, watermark pattern
- `context/sources/prodview/entity_model.md` — full entity hierarchy and mart design notes
- `context/sources/prodview/domains/completions.yaml` — pvUnitComp relationships
- `context/sources/prodview/domains/allocations.yaml` — allocation chain details
- `models/operations/marts/well_360/fct_well_production_monthly.sql` — EID resolution + aggregation pattern to follow
- `models/operations/platinum/well_360/plat_well__performance_scorecard.sql` — OBT expansion target
