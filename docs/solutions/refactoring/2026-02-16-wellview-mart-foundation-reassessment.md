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
| Expected null FK documentation | Partial pass | 3.5 | Runtime validation completed; warn-level tests confirm known nullability/coverage behavior (`report_date` warn=9, `wellbore_sk` relationship warn=468). |
| Dimensional filter coverage | Pass | 4 | Daily fact supports job/well/wellbore/date filtering; equipment dim now supports casing-aware filtering plus lift-type signals. |

## Validation Evidence (2026-02-16)

- `dbt build --select int_wellview__daily_drilling_enriched fct_daily_drilling dim_well_equipment`:
  - `PASS=20 WARN=2 ERROR=0`
- `dbt show --select fct_daily_drilling --limit 20` and `dbt show --select dim_well_equipment --limit 20` both succeeded.
- Spot-check reconciliation against existing marts:
  - Cost total delta (`fct_daily_drilling` vs `fct_daily_drilling_cost`): `+0.0071%`.
  - Time total delta (`fct_daily_drilling` vs `fct_drilling_time`): `-6.657%`, largely explained by rows with `job_report_id is null` in `fct_drilling_time` (`49,599` rows, `6.58%` of active rows).
  - NPT total delta (`fct_daily_drilling` vs `fct_npt_events`): `-3.307%`, attributable to report-level linkage limits and precedence behavior for date fallback vs report-number joins.

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
3. For cross-fact semantic KPIs, explicitly model or document treatment of non-report-linked time/NPT rows before publishing enterprise-wide metrics.
