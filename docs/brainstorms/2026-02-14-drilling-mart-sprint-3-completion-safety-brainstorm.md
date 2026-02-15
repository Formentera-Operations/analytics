# Brainstorm: Drilling Mart Sprint 3 — Completion Effectiveness + Safety

**Date:** 2026-02-14
**Status:** Ready for planning
**Participants:** Rob Stover, Claude

---

## What We're Building

Sprint 3 of the drilling/operations mart layer. Two fact tables:

1. **`fct_stimulation`** — One row per stimulation job. Aggregated metrics from the stim hierarchy (stages, proppant, fluids) plus perforation summary. FKs to well_360, dim_job, dim_wellbore.
2. **`fct_safety_events`** — Safety checks and incidents. Requires 2 new staging models first.

## Why This Approach

### Sprint 3 Failure Context

The original Sprint 3 plan called for `fct_rig_activity` and `fct_stand_performance` from calc tables (`wvJRigActivityCalc`, `wvJRStandCalc`). These tables **do not exist in Snowflake**. Fivetran only syncs 2 of 144 WellView calc tables (both directional survey composites). All code was reverted.

### Why Stimulation Next

With drilling cost, time, and NPT facts done (Sprint 2), the highest-value gap is **completion effectiveness** — how do frac designs correlate with well performance? Stimulation is the natural bridge between the drilling/operations side (already modeled) and the production side (ProdView, future).

- All 8 stimulation staging models already exist
- Rich hierarchy: 3,845 stim jobs → 123,992 stages → 67,517 proppant records → 22,397 fluid records
- Perforations (50,668 records) are tightly coupled with stim stages and already staged
- Dual membership in entity model: physical completion attribute + operational event

### Why Safety Too

Safety was originally Sprint 4 in the plan. The source tables have good data (57,857 checks + 4,711 incidents) and need only 2 new staging models. Including it here completes the Well Work fact layer.

### What's Deferred

- **Casing/cement configuration** — Physical Well entities, not Well Work. Already captured operationally via phase-level cost/time in Sprint 2 facts. Physical configuration belongs in a future "Well Configuration" sprint alongside `dim_well_equipment`.
- **Rig activity / stand KPIs** — Calc tables don't exist. Would need Fivetran to sync them (separate initiative) or accept the gap.
- **Equipment dimension** — Complex multi-entity build (casing + tubing + rods + perfs + prod settings + lift type inference). High value but different sprint.

## Key Decisions

1. **Star schema purity**: `fct_stimulation` contains measures and FKs only. No derived dimensional metrics (e.g., proppant/ft). Consumers join to `dim_wellbore` for lateral length and compute intensity metrics themselves. Derived calculations belong in a future semantic layer.

2. **Grain**: One row per stimulation job (not per stage). Stages, proppant, and fluids are aggregated via CTEs. This answers "how was this well completed?" without stage-by-stage granularity that's a niche use case.

3. **Perforations folded in**: Aggregated perf metrics (total_perfs, total_shots, perf_interval_top/bottom) included directly on `fct_stimulation` since perfs and stim are tightly coupled per well.

4. **No intermediate model**: Following Sprint 2 pattern — CTE aggregations within the fact. The stim hierarchy (4-5 source joins) fits in CTEs without needing a separate intermediate.

5. **Safety needs new staging**: 2 new staging models (`stg_wellview__safety_checks`, `stg_wellview__safety_incidents`) following the 5-CTE pattern established in the WellView staging refactor.

## Scope

### Models to Build

| Model | Type | Grain | Source | Rows (est.) |
|-------|------|-------|--------|-------------|
| `fct_stimulation` | Fact (table) | Stimulation job | 8 staged stim models + perf staging | ~3,845 |
| `stg_wellview__safety_checks` | Staging (view) | Safety check record | `WVT_WVJOBSAFETYCHK` | ~57,857 |
| `stg_wellview__safety_incidents` | Staging (view) | Safety incident | `WVT_WVJOBSAFETYINCIDENT` | ~4,711 |
| `fct_safety_events` | Fact (table) | Safety event | New staging models | ~62,568 |

### Dimensional Joins

`fct_stimulation` joins to:
- `well_360` via well_sk (IDWELL-based)
- `dim_job` via job_sk (if stim links to a job — need to verify FK chain)
- `dim_wellbore` via wellbore_sk (if wellbore tracked on stim)

`fct_safety_events` joins to:
- `well_360` via well_sk
- `dim_job` via job_sk (safety records are children of daily reports, which are children of jobs)

### YAML Documentation

- Schema YAML for all 4 models with column descriptions, PKs, and tests
- `unique` + `not_null` on primary keys
- `relationships` tests on FKs to existing dimensions

## Open Questions

1. **Stim → Job FK chain**: Stimulation hangs off Well (IDWELL), not Job. Is there a link from stim to the completion job? If not, the job_sk FK may be NULL or derived from date overlap. Need to check the staging models.

2. **Safety event grain**: Should checks and incidents be UNION'd into one `fct_safety_events` with an `event_type` discriminator, or kept as separate models? The original plan proposed UNION. Row counts are balanced enough (57K vs 4.7K) to make either work.

3. **Stim-to-well join path**: Stim records have IDWELL. Do they also carry a wellbore reference? If not, we join to well_360 directly and wellbore_sk may need to be derived or left NULL.

4. **Calc table gap**: Should we file a request with Fivetran/Peloton to sync more calc tables? The rig activity and stand KPI data would be genuinely valuable if available. This is a separate initiative from Sprint 3 but worth tracking.

## Future Sprints (Updated Roadmap)

| Sprint | Content | Status |
|--------|---------|--------|
| Sprint 1 | 4 dims (dim_job, dim_wellbore, dim_phase, bridge_job_afe) | **Merged** |
| Sprint 2 | 3 facts (fct_daily_drilling_cost, fct_drilling_time, fct_npt_events) | **Merged** |
| Sprint 3 | fct_stimulation + fct_safety_events (+ 2 staging models) | **This sprint** |
| Sprint 4 | Physical Well Configuration (dim_well_equipment, lift type inference) | Planned |
| Sprint 5 | Performance metrics + semantic layer (if calc tables become available) | Planned |

---

*Next step: Run `/workflows:plan` to generate implementation plan.*
