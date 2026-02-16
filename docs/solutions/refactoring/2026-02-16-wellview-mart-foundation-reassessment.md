---
title: "WellView Mart Foundation Reassessment After Daily-Report and Casing Coverage"
category: refactoring
tags: [wellview, marts, entity-model, semantic-readiness, drilling]
module: operations/marts/drilling
symptoms:
  - Missing canonical daily report fact for semantic metric anchoring
  - Incomplete equipment snapshot due to absent casing coverage
  - Unclear semantic readiness due to mixed-grain dependencies
date_solved: 2026-02-16
---

# WellView Mart Foundation Reassessment After Daily-Report and Casing Coverage

This checkpoint reflects state after:

- adding `fct_daily_drilling` (daily report grain)
- adding `int_wellview__daily_drilling_enriched` (daily aggregation bridge)
- extending `dim_well_equipment` with casing coverage

## Entity Model Completeness Scorecard

| Area | Status | Score (0-5) | Notes |
|---|---|---:|---|
| Physical Well | Partial | 4 | Core dimensions exist (`well_360`, `dim_wellbore`, `dim_well_survey`, `dim_zone`, `dim_well_equipment` now with casing/tubing/rod/perf). Still missing `fct_well_configuration` history and low-priority `dim_completion`. |
| Well Work | Strong | 4.5 | Canonical daily fact now exists (`fct_daily_drilling`) alongside cost/time/NPT/safety/stim facts. Primary operational entities are modeled at useful grains. |
| Cross-domain prerequisites | Partial | 2.5 | Cross-domain marts (`fct_well_cost_vs_budget`, ownership/economics bridges) remain unbuilt; these are not blockers for drilling-only semantic views but do block broader enterprise semantic coverage. |

## Semantic Readiness Scorecard

| Criterion | Status | Score (0-5) | Notes |
|---|---|---:|---|
| Stable grain | Pass | 4.5 | `fct_daily_drilling` establishes one-row-per-report anchor to reduce mixed-grain metric risk. |
| Conformed keys | Pass | 4 | EID resolution remains standardized via `well_360.wellview_id`; job/wellbore surrogate key patterns are consistent. |
| Additive measure definitions | Pass | 4 | Daily child measures are pre-aggregated and additive at report grain (cost/time/NPT/safety rollups). |
| Expected null FK documentation | Partial pass | 3.5 | Existing schema patterns use warn-level relationships for nullable FKs; still needs runtime validation after Snowflake connectivity is restored. |
| Dimensional filter coverage | Pass | 4 | Daily fact supports job/well/wellbore/date filtering; equipment dim now supports casing-aware filtering plus lift-type signals. |

## Go / No-Go Decision

**Decision: GO (Scoped)**

Proceed with Cortex semantic modeling for the drilling operational surface area built on:

- `fct_daily_drilling`
- `fct_daily_drilling_cost`
- `fct_drilling_time`
- `fct_npt_events`
- `fct_safety_events`
- `fct_stimulation`
- `dim_job`
- `dim_phase`
- `dim_wellbore`
- `dim_well_equipment`
- `dim_well_survey`
- `dim_zone`

**Caveats before broad/full semantic rollout:**

1. Build or explicitly defer `fct_well_configuration` if effective-dated physical configuration analysis is required.
2. Build or explicitly defer `fct_drilling_performance` for stand/slide/rotate KPI semantics.
3. Re-run `dbt build/show` validations when Snowflake connectivity is available; current session had repeated `250001` backend connection failures.
