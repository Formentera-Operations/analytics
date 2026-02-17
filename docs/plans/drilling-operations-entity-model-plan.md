# Plan: Drilling Operations Entity Model

## Context

WellView has 144 calc tables that encode the vendor's entity model for drilling analytics. We've documented these in `context/sources/wellview/calc_table_blueprint.md`. This plan turns that blueprint into a concrete mart layer: dimensions, facts, and a path to a semantic layer.

**Design decision:** Option C (Hybrid) — Build facts from base tables for cost and time (full flexibility), stage calc tables directly for rig activity and stand KPIs (sensor-derived, impractical to rebuild), use calc tables as validation targets everywhere.

### What Already Exists

| Layer | Model | Status | Notes |
|---|---|---|---|
| **Dimension** | `well_360` | Enabled | Golden record, 5-source spine. Use as FK, don't duplicate. |
| **Dimension** | `dim_wells` (finance) | Enabled | ODA-only, pre-dates well_360. Finance team dependency. |
| **Dimension** | `dim_afes` (finance) | Enabled | ODA-sourced AFE dimension. Drilling AFEs from WellView not yet integrated. |
| **Fact** | `fct_eng_jobs` | Enabled | Job-grain, basic attributes + AFE amounts. 3-year filter. |
| **Fact** | `fct_job_performance` | **Disabled** | Phase-grain, NPT + cost aggregations. Shows intended structure. |
| **Intermediate** | `int_wellview_job` | Enabled | Jobs joined to well header (well_code, API-10). |
| **Intermediate** | `int_wellview__canonical_wells` | Enabled | Deduped wellbore + status per well. |
| **Staging** | 61 WellView models | Enabled | Jobs, reports, costs, time logs, phases, wellbores, surveys, equipment, perfs, stims, etc. |

### What's Missing (This Plan Fills)

- No **wellbore dimension** (sidetrack/lateral tracking)
- No **drilling cost fact** at daily grain (daily → job → well rollup)
- No **drilling time fact** (time log entries)
- No **rig activity fact** (sensor-derived states — this is where calc tables shine)
- No **NPT fact** (interval problems)
- No **safety fact** (incidents + checks)
- No **stand performance fact** (connection times, ROP per stand)
- No **job-AFE bridge** (M:M: a job can have multiple AFEs, an AFE can fund multiple jobs)
- No **drilling performance metrics** (ROP, cost/ft, NPT%, productive time %)

---

## Entity Model

### Two Hierarchies

```
TEMPORAL (how work happens)              PHYSICAL (where work happens)
─────────────────────────                ──────────────────────────────
Well (well_360)                          Well (well_360)
  └── Job (dim_job)                        └── Wellbore (dim_wellbore) — NEW
        └── Phase (dim_phase) — NEW              └── Formation (future)
              └── Daily Report (grain)                 └── Zone (future)
                    └── Time Log (grain)
                          └── Stand (grain)
```

Every fact table joins to both hierarchies. The temporal axis tells you *when* and *what operation*; the physical axis tells you *where in the well*.

### Dimensions

#### `dim_job` — NEW (alongside `fct_eng_jobs`)

New job dimension. `fct_eng_jobs` stays untouched; `dim_job` lives alongside it with richer attributes and no 3-year filter. Downstream consumers migrate over time.

| Attribute | Source | Notes |
|---|---|---|
| job_sk | Generated | Surrogate key |
| well_sk | `well_360` | FK to well dimension |
| job_id | `stg_wellview__jobs.id_rec` | WellView IDRec |
| job_number | `stg_wellview__jobs` | Human-readable job number |
| job_type_primary | `stg_wellview__jobs` | Drilling, Completion, Workover, etc. |
| job_type_secondary | `stg_wellview__jobs` | Sub-classification |
| job_objective | `stg_wellview__jobs` | Free text objective |
| rig_name | `stg_wellview__rigs` | Join via IDRecParent |
| rig_contractor | `stg_wellview__rigs` | Rig contractor name |
| planned_start_date | `stg_wellview__jobs` | |
| actual_start_date | `stg_wellview__jobs` | |
| actual_end_date | `stg_wellview__jobs` | |
| job_duration_days | Derived | end - start |
| total_depth_md | `stg_wellview__jobs` | Depth at job end |
| afe_number_primary | `stg_wellview__job_afe_definitions` | Primary AFE (if single) |
| afe_amount_primary | `stg_wellview__job_afe_definitions` | Primary AFE budget |
| is_active | Derived | No end date or recent activity |
| wellbore_id | `stg_wellview__jobs` | FK to wellbore if tracked |

*No time filter — dimensions should be complete. `fct_eng_jobs` keeps its own 3-year filter independently.*

#### `dim_wellbore` — NEW

| Attribute | Source | Notes |
|---|---|---|
| wellbore_sk | Generated | |
| well_sk | `well_360` | FK |
| wellbore_id | `stg_wellview__wellbores.id_rec` | |
| wellbore_name | `stg_wellview__wellbores` | |
| wellbore_number | `stg_wellview__wellbores` | Sidetrack number |
| profile | `stg_wellview__wellbores` | Vertical, Directional, Horizontal |
| proposed_or_actual | `stg_wellview__wellbores` | Filter to Actual for marts |
| md_total | `stg_wellview__wellbore_depths` | Total measured depth |
| tvd_total | `stg_wellview__wellbore_depths` | Total vertical depth |
| kickoff_depth | `stg_wellview__wellbore_depths` | KOP for directional wells |
| landing_point_depth | `stg_wellview__wellbore_depths` | Horizontal landing point |
| spud_date | `stg_wellview__wellbores` | |
| td_date | `stg_wellview__wellbores` | |

#### `dim_phase` — NEW

| Attribute | Source |
|---|---|
| phase_sk | Generated |
| job_sk | FK to dim_job |
| phase_id | `stg_wellview__job_program_phases.id_rec` |
| phase_name | `stg_wellview__job_program_phases` |
| phase_type_1 | Code 1 classification |
| phase_type_2 | Code 2 classification |
| planned_start_date | |
| planned_end_date | |
| actual_start_date | |
| actual_end_date | |
| planned_depth_start | |
| planned_depth_end | |
| actual_depth_start | |
| actual_depth_end | |
| planned_duration_days | Derived |
| actual_duration_days | Derived |
| duration_variance_days | actual - planned |

#### `bridge_job_afe` — NEW

Many-to-many: a job can have multiple AFEs, an AFE can fund multiple jobs.

| Column | Source |
|---|---|
| job_sk | FK to dim_job |
| afe_id | `stg_wellview__job_afe_definitions` |
| afe_number | |
| afe_amount | Budget amount |
| afe_supplement_amount | Supplemental budget |
| cost_type | AFE cost classification |
| is_primary | Flag for primary AFE |

*Links WellView job AFEs to the ODA `dim_afes` via AFE number matching (future cross-system bridge).*

### Fact Tables

#### `fct_daily_drilling_cost` — NEW (Build from Base Tables)

**Grain:** One row per daily report x cost line item.
**Source:** `stg_wellview__daily_costs` + `stg_wellview__daily_recurring_costs` (UNION).
**Validation:** Compare job-level rollups to `wvJCostCumCalc`.

| Measure / Dimension | Source | Notes |
|---|---|---|
| well_sk | via dim_job | FK |
| job_sk | FK | |
| phase_sk | FK (if allocatable) | |
| wellbore_sk | FK (if on job) | |
| report_date | `stg_wellview__job_reports` | Daily report date |
| cost_description | | Free text |
| code_1 through code_6 | | 6-level cost hierarchy |
| vendor | | |
| afe_number | | If allocated to specific AFE |
| ops_category | | Operations category |
| po_number | | Purchase order |
| ticket_number | | |
| field_estimate_amount | | Primary cost measure |
| is_recurring | | Flag: daily vs recurring cost |

**Key aggregations this enables:**
- Daily burn rate (sum by report_date)
- Cost by vendor / AFE / cost code hierarchy
- Phase-level cost (sum within phase date range)
- Job-level cost (sum by job)
- Well-level cost (sum by well, across jobs)
- AFE variance (join to `bridge_job_afe` for budget vs actual)

#### `fct_drilling_time` — NEW (Build from Base Tables)

**Grain:** One row per time log entry.
**Source:** `stg_wellview__job_time_log`.
**Validation:** Compare job-level sums to `wvJTLSumCalc`.

| Measure / Dimension | Source |
|---|---|
| well_sk | via dim_job |
| job_sk | FK |
| phase_sk | FK (derived from time range overlap with phase) |
| report_date | From parent daily report |
| start_datetime | Time log start |
| end_datetime | Time log end |
| duration_hours | |
| code_1 through code_4 | Time classification hierarchy |
| ops_category | Productive / NPT / flat time |
| unscheduled_type | If unscheduled, what type |
| depth_start | MD at start |
| depth_end | MD at end |
| is_npt | Derived: ops_category indicates NPT |
| is_productive | Derived: on-bottom drilling time |

**Key aggregations:**
- Productive time % (productive hours / total hours)
- NPT % and hours by type
- Time breakdown by ops category
- Phase duration variance (actual time log vs planned)
- Flat time analysis

#### `fct_rig_activity` — NEW (Stage from Calc Tables)

**Grain:** One row per rig activity state at job grain.
**Source:** Stage `wvJRigActivityCalc` directly (sensor-derived, impractical to rebuild from raw sensor data).
**Enrichment:** Join to dim_job, well_360 for context.

| Measure / Dimension | Source Column |
|---|---|
| well_sk | via IDWELL |
| job_sk | via IDRecParent |
| activity_type_1 | Typ1 |
| duration_total_days | Duration |
| pct_total_time | FractionTotalDur |
| duration_on_bottom_days | DurOnBtm |
| duration_off_bottom_days | DurOffBtm |
| duration_pipe_moving_days | DurPipeMoving |
| depth_start_m | DepthStart |
| depth_end_m | DepthEnd |
| rop_m_per_day | ROPStartEnd |
| count_occurrences | Count |
| wob_mean / max / min / stddev | WOB* |
| rpm_mean / max / min / stddev | RPM* |
| torque_mean / max / min | Torque* |
| spp_mean / max / min | SPP* |
| flow_rate_mean / max / min | LiquidInjRate* |

*Start at job grain. Add daily report grain (`wvJRRigActivityCalc`) and drill string grain (`wvJDSRigActivityCalc`) when needed.*

#### `fct_npt_events` — NEW (Build from Base Tables)

**Grain:** One row per NPT event.
**Source:** `stg_wellview__job_interval_problems`.
**Validation:** Compare to `wvJRIntervalProblemSumCalc`.

| Measure / Dimension | Source |
|---|---|
| well_sk | via dim_job |
| job_sk | FK |
| phase_sk | FK (derived) |
| report_date | When NPT started |
| problem_type | Category classification |
| major_category | High-level grouping |
| duration_hours | Problem duration |
| depth_start | Where it occurred |
| depth_end | |
| exclude_from_calcs | WellView exclusion flag |
| description | Free text |
| cost_estimate | If tracked |

#### `fct_safety_events` — NEW (Build from Base + Calc)

**Grain:** One row per safety incident or check.
**Source:** `wvJobSafetyIncident` (needs staging model) + `wvJobSafetyCheck` (needs staging model).
**Note:** These base tables are NOT yet staged — 2 new staging models needed.

| Measure / Dimension | Source |
|---|---|
| well_sk | via dim_job |
| job_sk | FK |
| report_date | Date of event |
| event_type | 'incident' or 'check' |
| incident_type_1 | Classification |
| incident_type_2 | Sub-classification |
| is_reportable | OSHA reportable flag |
| frequency | Occurrence count |
| days_since_last | Calc-derived |

#### `fct_stand_performance` — NEW (Stage from Calc Tables)

**Grain:** One row per stand at daily report grain.
**Source:** Stage `wvJRStandCalc` directly (pre-computed KPIs from sensor data).

| Measure / Dimension | Source Column |
|---|---|
| well_sk | via IDWELL |
| job_sk | via IDRecParent chain |
| report_date | From parent daily report |
| stand_duration_avg | DurStand |
| stand_duration_min / max / med | DurStandMin/Max/Med |
| connection_time_avg | DurConnect |
| connection_time_min / max / med | DurConnectMin/Max/Med |
| on_bottom_avg | DurOnBtm |
| off_bottom_avg | DurOffBtm |

---

## Cross-System Integration Points

The drilling entity model connects to existing ProdView and ODA marts at these points:

| Join Point | From | To | Via |
|---|---|---|---|
| **Well** | All drilling facts | `well_360` | `well_sk` (EID-based spine) |
| **AFE** | `bridge_job_afe` | `dim_afes` (ODA) | AFE number matching |
| **Cost Codes** | `fct_daily_drilling_cost.code_1-6` | `int_accounts_classified` | GL account code mapping |
| **Completion** | `dim_wellbore` / `dim_phase` | ProdView completions | `int_prodview__well_header` (via system_integrations) |
| **Production** | Zone production calcs | ProdView daily allocations | Zone ↔ completion linkage |

**Future ontology backbone:** `well_360.eid` → `dim_job.well_sk` → all drilling facts. Same EID links to ProdView production and ODA financials. The well is the universal entity.

---

## Implementation Sprints

### Sprint 1: Core Dimensions (Foundation)

Build the dimensional backbone. No new staging needed — everything sources from existing models.

- [ ] `dim_job` — new, alongside `fct_eng_jobs` (enrich with rig, wellbore; no time filter)
- [ ] `dim_wellbore` — new (from `stg_wellview__wellbores` + `stg_wellview__wellbore_depths`)
- [ ] `dim_phase` — new (from `stg_wellview__job_program_phases`)
- [ ] `bridge_job_afe` — new (from `stg_wellview__job_afe_definitions`)
- [ ] Delete disabled `fct_job_performance` (scrapped — superseded by this plan)

**Validation:** Row counts match source; surrogate keys are unique; FKs resolve.

### Sprint 2: Cost & Time Facts (Highest Business Value)

The two facts that replace the most calc tables and unlock the most reporting.

- [ ] `fct_daily_drilling_cost` — UNION of daily + recurring costs, joined to dimensions
- [ ] `fct_drilling_time` — from job time log, joined to dimensions
- [ ] Validation tests against `wvJCostCumCalc` and `wvJTLSumCalc`

**Validation:** Job-level cost sums match WellView calc tables within tolerance. Time durations sum to 24h per report day.

### Sprint 3: Rig Activity & Stand KPIs (Calc Table Staging)

New staging models for calc tables + fact tables.

- [ ] `stg_wellview__job_rig_activity_calc` — stage `wvJRigActivityCalc`
- [ ] `stg_wellview__daily_stand_kpis_calc` — stage `wvJRStandCalc`
- [ ] `fct_rig_activity` — from staged calc table + dimension joins
- [ ] `fct_stand_performance` — from staged calc table + dimension joins

### Sprint 4: NPT & Safety (Operational Reporting)

- [ ] `fct_npt_events` — from existing `stg_wellview__job_interval_problems`
- [ ] `stg_wellview__safety_incidents` — new staging model (base table not yet staged)
- [ ] `stg_wellview__safety_checks` — new staging model (base table not yet staged)
- [ ] `fct_safety_events` — from new staging models

### Sprint 5: Performance Metrics & Semantic Layer

Derived metric models that combine the above facts.

- [ ] `fct_job_performance_summary` — one row per job with key KPIs
  - Total cost, AFE variance, NPT hours, productive time %, avg ROP, avg connection time, TRIR
- [ ] Semantic layer definitions (dbt Semantic Layer YAML)
  - `semantic_model: drilling_costs`
  - `semantic_model: drilling_time`
  - `semantic_model: drilling_performance`

---

## What This Does NOT Do

- Does not touch ProdView or ODA models (builds alongside, joins via well_360)
- Does not build formation or zone dimensions yet (future, when geological analysis is needed)
- Does not stage all 144 calc tables (only the 2-4 that are sensor-derived and impractical to rebuild)
- Does not create a UI or dashboard (mart layer only)
- Does not touch `fct_eng_jobs` — it stays as-is; `dim_job` lives alongside it

## Decisions (Resolved)

1. **`fct_eng_jobs`:** Keep as-is, do not touch. Create `dim_job` alongside it. Deprecate `fct_eng_jobs` at a future date once `dim_job` is validated and downstream consumers migrate.
2. **AFE cross-system bridge:** In-house AFEs should tie out across WellView and ODA. Historical acquisition AFEs (loaded from prior operators) may not match — the bridge will have gaps for legacy data. Accept this; don't force-match.
3. **Cost normalization:** All costs are USD today. Ignore `Normalized` calc table variants for now. Future global operations may require currency normalization — but do not build for that yet (YAGNI).
4. **`fct_job_performance`:** Scrap it. The disabled model's approach is superseded by this plan's fact tables. Delete in Sprint 1 cleanup.
5. **`dim_job` scope:** No 3-year filter. Dimensions should be complete. `fct_eng_jobs` keeps its filter independently.

## References

- `context/sources/wellview/calc_table_blueprint.md` — full calc table inventory, grain map, metric catalog
- `context/sources/wellview/wellview.md` — WellView system overview, join patterns, gotchas
- `context/sources/wellview/operations_jobs.yaml` — column-level schema for all job/report/phase/DS calc tables
- `models/operations/marts/well_360.sql` — existing well dimension (golden record pattern)
- `models/operations/marts/production/fct_eng_jobs.sql` — existing job fact (to be replaced by dim_job)
- `models/operations/marts/fct_job_performance.sql` — disabled performance fact (prior art)
