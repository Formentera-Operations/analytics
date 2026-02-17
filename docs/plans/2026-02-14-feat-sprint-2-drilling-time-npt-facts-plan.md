---
title: "Sprint 2: fct_drilling_time + fct_npt_events"
type: feat
date: 2026-02-14
sprint: 2
branch: feature/sprint-3-wellview-staging-cleanup
brainstorm: docs/brainstorms/2026-02-14-wellview-intermediate-mart-modeling-brainstorm.md
---

## Enhancement Summary

**Deepened on:** 2026-02-14
**Agents used:** learnings-researcher, architecture-strategist, data-integrity-guardian, performance-oracle

### Key Improvements from Deepen-Plan

1. **Clustering added** — `cluster_by=['job_id', 'report_date']` on `fct_drilling_time` for micro-partition pruning on common query patterns (by-job, by-date analysis)
2. **`is_report_date_inferred` flag** — Boolean column to indicate when report_date fell back to `start_datetime::date` (aids data quality monitoring)
3. **Negative duration guard** — `dbt_expectations.expect_column_values_to_be_between` test on NPT durations (min_value=0) to catch data quality issues
4. **Not-null tests on natural FKs** — `job_id`, `parent_record_id`, `well_id` all get `not_null` tests (not just surrogate keys)
5. **Explicit final CTE** — Final CTE uses explicit column list (not `select *`) per Sprint 1 pattern
6. **Phase_sk validation query** — Added cross-reference check between time log `job_program_phase_id` and `dim_phase.record_id`

### Findings Not Incorporated (with rationale)

- **Incremental materialization for fct_drilling_time** — Performance oracle recommended incremental at 762K rows. Kept as table per brainstorm decision: `_loaded_at = current_timestamp()` in staging views makes watermark filter a no-op anyway. Merge on natural key still works but adds complexity with no benefit. Revisit at ~5M rows.
- **well_360 wellview_id uniqueness test** — Already tested in well_360's own schema YAML. Not duplicated here.

---

# Sprint 2: `fct_drilling_time` + `fct_npt_events`

## Overview

Build two drilling fact tables that answer **"How did we spend our time?"** and **"What went wrong?"** — the second and third highest-value questions from the drilling marts brainstorm.

Both models are **single-source facts** (no intermediates needed). Each sources directly from one staging model with CTE joins for dimensional FKs (`job_sk` via inline computation, `eid` via `well_360`).

## Models to Build

| # | Model | Layer | Materialization | Source Staging | Rows |
|---|-------|-------|-----------------|----------------|------|
| 1 | `fct_drilling_time` | Mart/Fact | table | `stg_wellview__job_time_log` | ~762K |
| 2 | `fct_npt_events` | Mart/Fact | table | `stg_wellview__job_interval_problems` | ~14K |

**No new directories needed.** Both models go in `models/operations/marts/drilling/` (alongside existing `dim_job`, `dim_wellbore`, `dim_phase`, `bridge_job_afe`, `fct_daily_drilling_cost`).

**No intermediates needed.** Both staging models have `job_id` (or equivalent) and `well_id` directly available. The FK enrichment (job_sk, eid) is simple enough for CTE joins within the mart.

---

## Task 1: `fct_drilling_time`

### Purpose

Time breakdown fact table. Every activity entry from WellView daily reports — drilling, tripping, circulating, NPT, etc. — with duration, depth, ROP, and activity classification.

### Source

`stg_wellview__job_time_log` — grain: one row per time log entry (PK: `time_log_id`)

### FK Resolution (CTE joins within mart)

| FK | Source | Join |
|----|--------|------|
| `job_sk` | Computed inline | `generate_surrogate_key(['tl.job_id'])` |
| `phase_sk` | Computed inline | `generate_surrogate_key(['tl.job_program_phase_id'])` |
| `eid` | `well_360` | `tl.well_id = w360.wellview_id` |
| `report_date` | `stg_wellview__job_reports` | `tl.job_report_id = jr.report_id` → fallback to `start_datetime::date` |

**Verification needed:** Confirm `job_id` (IDRECPARENT) values match `stg_wellview__jobs.job_id` (IDRec). Run `dbt show` validation query during implementation.

### Output Columns (explicit contract)

```sql
-- surrogate key
job_time_log_sk        -- from staging (passthrough)

-- dimensional FKs
job_sk                 -- computed inline (MD5 of job_id)
phase_sk               -- computed inline (MD5 of job_program_phase_id)
eid                    -- from well_360

-- natural keys
time_log_id            -- PK (unique per time log entry)
job_id                 -- WellView job GUID (FK to dim_job natural key)
well_id                -- WellView well GUID
job_report_id          -- FK to daily report
wellbore_id            -- FK to dim_wellbore

-- temporal
report_date            -- DATE: from job_reports, fallback to start_datetime::date
is_report_date_inferred -- BOOLEAN: true when report_date fell back to start_datetime
start_datetime         -- activity start
end_datetime           -- activity end
days_from_spud         -- days since spud

-- duration measures
duration_hours         -- total elapsed time for this activity
problem_time_hours     -- NPT portion of this activity
no_problem_time_hours  -- productive portion of this activity
on_bottom_duration_minutes   -- drilling on-bottom time
off_bottom_duration_minutes  -- off-bottom time
pipe_moving_duration_minutes -- pipe movement time

-- activity classification
time_log_code_1        -- primary code (FRAC, FLOWBACK, PRODUCTION DRILLING, etc.)
time_log_code_2        -- secondary code
time_log_code_3        -- tertiary code
time_log_code_4        -- quaternary code
ops_category           -- operational categorization
unscheduled_type       -- unscheduled activity classification

-- depth
start_depth_ft         -- MD at activity start
end_depth_ft           -- MD at activity end
start_depth_tvd_ft     -- TVD at activity start
end_depth_tvd_ft       -- TVD at activity end

-- performance
rop_ft_per_hour        -- rate of penetration
wellbore_size_inches   -- hole size at time of activity

-- context
formation              -- formation at time of activity
report_number          -- daily report number
is_problem_time        -- boolean: NPT flag
comments               -- free text activity description

-- report tracking
rig_days               -- rig day count
cumulative_rig_days    -- cumulative rig days

-- dbt metadata
_loaded_at
```

### SQL Pattern

```sql
{{
    config(
        materialized='table',
        cluster_by=['job_id', 'report_date'],
        tags=['drilling', 'mart', 'fact']
    )
}}

with time_log as (
    select * from {{ ref('stg_wellview__job_time_log') }}
    where not coalesce(is_inactive, false)
),

job_reports as (
    select
        report_id,
        report_start_datetime::date as report_date
    from {{ ref('stg_wellview__job_reports') }}
),

well_360 as (
    select wellview_id, eid
    from {{ ref('well_360') }}
    where wellview_id is not null
),

enriched as (
    select
        tl.job_time_log_sk,

        -- dimensional FKs
        {{ dbt_utils.generate_surrogate_key(['tl.job_id']) }} as job_sk,
        {{ dbt_utils.generate_surrogate_key(['tl.job_program_phase_id']) }} as phase_sk,
        w360.eid,

        -- natural keys
        tl.time_log_id,
        tl.job_id,
        tl.well_id,
        tl.job_report_id,
        tl.wellbore_id,

        -- temporal
        coalesce(jr.report_date, tl.start_datetime::date) as report_date,
        jr.report_id is null as is_report_date_inferred,
        tl.start_datetime,
        tl.end_datetime,
        tl.days_from_spud,

        -- duration measures
        tl.duration_hours,
        tl.problem_time_hours,
        tl.no_problem_time_hours,
        tl.on_bottom_duration_minutes,
        tl.off_bottom_duration_minutes,
        tl.pipe_moving_duration_minutes,

        -- activity classification
        tl.time_log_code_1,
        tl.time_log_code_2,
        tl.time_log_code_3,
        tl.time_log_code_4,
        tl.ops_category,
        tl.unscheduled_type,

        -- depth
        tl.start_depth_ft,
        tl.end_depth_ft,
        tl.start_depth_tvd_ft,
        tl.end_depth_tvd_ft,

        -- performance
        tl.rop_ft_per_hour,
        tl.wellbore_size_inches,

        -- context
        tl.formation,
        tl.report_number,
        tl.is_problem_time,
        tl.comments,

        -- report tracking
        tl.rig_days,
        tl.cumulative_rig_days,

        -- dbt metadata
        tl._loaded_at

    from time_log as tl
    left join job_reports as jr
        on tl.job_report_id = jr.report_id
    left join well_360 as w360
        on tl.well_id = w360.wellview_id
),

final as (
    select
        job_time_log_sk,
        job_sk,
        phase_sk,
        eid,
        time_log_id,
        job_id,
        well_id,
        job_report_id,
        wellbore_id,
        report_date,
        is_report_date_inferred,
        start_datetime,
        end_datetime,
        days_from_spud,
        duration_hours,
        problem_time_hours,
        no_problem_time_hours,
        on_bottom_duration_minutes,
        off_bottom_duration_minutes,
        pipe_moving_duration_minutes,
        time_log_code_1,
        time_log_code_2,
        time_log_code_3,
        time_log_code_4,
        ops_category,
        unscheduled_type,
        start_depth_ft,
        end_depth_ft,
        start_depth_tvd_ft,
        end_depth_tvd_ft,
        rop_ft_per_hour,
        wellbore_size_inches,
        formation,
        report_number,
        is_problem_time,
        comments,
        rig_days,
        cumulative_rig_days,
        _loaded_at
    from enriched
)

select * from final
```

### Design Decisions

1. **Table materialization** — 762K rows is manageable for full rebuild. Convert to incremental if growth exceeds ~5M rows.
2. **Filter inactive entries** — `is_inactive = true` entries are excluded (status flag from WellView).
3. **No zero-duration filter** — Keep zero-duration entries; they're valid status markers. The brainstorm noted 8 null-code-1 Completion entries with 0 duration — these pass through. Consumers filter as needed.
4. **Activity codes pass through as-is** — No reclassification. `time_log_code_1` values (FRAC, FLOWBACK, PRODUCTION DRILLING, etc.) are source-driven.
5. **report_date with fallback** — `coalesce(jr.report_date, tl.start_datetime::date)` uses job report date when available, falls back to activity start date when `job_report_id` is NULL.
6. **phase_sk for phase analysis** — Computed inline from `job_program_phase_id`. Enables drilling time analysis by phase (surface, intermediate, horizontal).
7. **Additional analyst columns** — `wellbore_id` (wellbore analysis), `comments` (keyword search), `wellbore_size_inches` (hole size analysis) included per spec-flow review.

---

## Task 2: `fct_npt_events`

### Purpose

Non-productive time (NPT) event fact table. Every incident from WellView interval problems — equipment failures, formation issues, weather delays — with duration, cost, severity, and categorization.

### Source

`stg_wellview__job_interval_problems` — grain: one row per NPT event (PK: `record_id`)

### FK Resolution (CTE joins within mart)

| FK | Source | Join |
|----|--------|------|
| `job_sk` | Computed inline | `generate_surrogate_key(['ip.parent_record_id'])` |
| `eid` | `well_360` | `ip.well_id = w360.wellview_id` |

**Verification needed:** Confirm `parent_record_id` (IDRECPARENT) matches `stg_wellview__jobs.job_id` (IDRec). Run `dbt show` validation query during implementation.

### Output Columns (explicit contract)

```sql
-- surrogate key
job_interval_problem_sk  -- from staging (passthrough)

-- dimensional FKs
job_sk                   -- computed inline (MD5 of parent_record_id)
eid                      -- from well_360

-- natural keys
record_id                -- PK (unique per NPT event)
parent_record_id         -- job GUID (IDRECPARENT)
well_id                  -- WellView well GUID
wellbore_id              -- FK to dim_wellbore

-- temporal
event_date               -- DATE: start_date::date (when NPT began)
start_date               -- event start datetime
end_date                 -- event end datetime
days_from_spud           -- days since spud

-- duration measures
problem_duration_gross_hours    -- total event duration
problem_duration_net_hours      -- duration after adjustments
estimated_lost_time_hours       -- schedule impact

-- cost measures
problem_cost                    -- financial impact
cost_recovery                   -- recovered amount

-- categorization
major_category           -- high-level grouping (e.g., "rig equip")
problem_type             -- specific type (e.g., "Surface Equipment")
problem_subtype          -- detailed sub-classification
severity                 -- severity rating
potential_severity       -- potential severity rating
status                   -- current problem status (open/closed)

-- operational context
operative_condition      -- operational state when problem occurred
accountable_party        -- who/what caused it
action_taken             -- remediation action
formation                -- formation at time of problem
rig_crew_name            -- crew on duty
description              -- free text detail

-- depth
start_depth_ft           -- MD at event start
end_depth_ft             -- MD at event end
start_depth_tvd_ft       -- TVD at event start
end_depth_tvd_ft         -- TVD at event end

-- report context
report_number            -- daily report number
report_day               -- report day count

-- flags
exclude_from_problem_time_calculations  -- WellView exclusion flag

-- dbt metadata
_loaded_at
```

### SQL Pattern

```sql
{{
    config(
        materialized='table',
        tags=['drilling', 'mart', 'fact']
    )
}}

with interval_problems as (
    select * from {{ ref('stg_wellview__job_interval_problems') }}
),

well_360 as (
    select wellview_id, eid
    from {{ ref('well_360') }}
    where wellview_id is not null
),

enriched as (
    select
        ip.job_interval_problem_sk,

        -- dimensional FKs
        {{ dbt_utils.generate_surrogate_key(['ip.parent_record_id']) }} as job_sk,
        w360.eid,

        -- natural keys
        ip.record_id,
        ip.parent_record_id,
        ip.well_id,
        ip.wellbore_id,

        -- temporal
        ip.start_date::date as event_date,
        ip.start_date,
        ip.end_date,
        ip.days_from_spud,

        -- duration measures
        ip.problem_duration_gross_hours,
        ip.problem_duration_net_hours,
        ip.estimated_lost_time_hours,

        -- cost measures
        ip.problem_cost,
        ip.cost_recovery,

        -- categorization
        ip.major_category,
        ip.problem_type,
        ip.problem_subtype,
        ip.severity,
        ip.potential_severity,
        ip.status,

        -- operational context
        ip.operative_condition,
        ip.accountable_party,
        ip.action_taken,
        ip.formation,
        ip.rig_crew_name,
        ip.description,

        -- depth
        ip.start_depth_ft,
        ip.end_depth_ft,
        ip.start_depth_tvd_ft,
        ip.end_depth_tvd_ft,

        -- report context
        ip.report_number,
        ip.report_day,

        -- flags
        ip.exclude_from_problem_time_calculations,

        -- dbt metadata
        ip._loaded_at

    from interval_problems as ip
    left join well_360 as w360
        on ip.well_id = w360.wellview_id
),

final as (
    select
        job_interval_problem_sk,
        job_sk,
        eid,
        record_id,
        parent_record_id,
        well_id,
        wellbore_id,
        event_date,
        start_date,
        end_date,
        days_from_spud,
        problem_duration_gross_hours,
        problem_duration_net_hours,
        estimated_lost_time_hours,
        problem_cost,
        cost_recovery,
        major_category,
        problem_type,
        problem_subtype,
        severity,
        potential_severity,
        status,
        operative_condition,
        accountable_party,
        action_taken,
        formation,
        rig_crew_name,
        description,
        start_depth_ft,
        end_depth_ft,
        start_depth_tvd_ft,
        end_depth_tvd_ft,
        report_number,
        report_day,
        exclude_from_problem_time_calculations,
        _loaded_at
    from enriched
)

select * from final
```

### Design Decisions

1. **Table materialization** — 14K rows is trivial for full rebuild.
2. **No filtering on exclude flag** — `exclude_from_problem_time_calculations` passes through as a flag. Consumers decide whether to exclude.
3. **No is_inactive filter** — Unlike time logs, interval problems have no `is_inactive` column. The `exclude_from_problem_time_calculations` flag is the only exclusion mechanism.
4. **event_date derived from start_date** — `start_date::date` is the NPT event date. No join to job_reports needed.
5. **Cost columns included** — `problem_cost` and `cost_recovery` pass through. Not all events have costs populated.
6. **parent_record_id = job_id assumption** — IDRECPARENT maps to the parent job's IDRec. Validate during implementation.
7. **Additional analyst columns** — `wellbore_id` (wellbore analysis), `formation` (root cause by formation), `status` (open/closed filtering), `rig_crew_name` (crew performance) included per spec-flow review.

---

## Task 3: Schema YAML

### Addition to `models/operations/marts/drilling/schema.yml`

```yaml
  - name: fct_drilling_time
    description: >
      Drilling time fact table. One row per time log entry from WellView daily
      reports. Includes activity duration, classification codes, depth, ROP,
      and NPT flag. Enriched with job SK and well EID. ~762K rows.
    columns:
      - name: time_log_id
        description: WellView time log entry natural key (IDRec)
        data_tests:
          - unique
          - not_null

      - name: job_id
        description: WellView job GUID (natural FK to dim_job)
        data_tests:
          - not_null

      - name: well_id
        description: WellView well GUID
        data_tests:
          - not_null

      - name: job_sk
        description: FK to dim_job surrogate key
        data_tests:
          - relationships:
              arguments:
                to: ref('dim_job')
                field: job_sk
              config:
                severity: warn

      - name: phase_sk
        description: FK to dim_phase surrogate key
        data_tests:
          - relationships:
              arguments:
                to: ref('dim_phase')
                field: phase_sk
              config:
                severity: warn

      - name: eid
        description: Well entity identifier — FK to well_360
        data_tests:
          - relationships:
              arguments:
                to: ref('well_360')
                field: eid
              config:
                severity: warn

      - name: report_date
        description: Date of the daily report (with fallback to activity start date)
        data_tests:
          - not_null:
              config:
                severity: warn

      - name: duration_hours
        description: Total elapsed time for this activity (hours)
        data_tests:
          - not_null:
              config:
                severity: warn

      - name: is_report_date_inferred
        description: True when report_date fell back to start_datetime (no matching job report)

      - name: is_problem_time
        description: Whether this activity is non-productive time (NPT)

  - name: fct_npt_events
    description: >
      Non-productive time (NPT) event fact table. One row per interval problem
      from WellView. Includes event duration (gross/net), cost, severity, and
      categorization hierarchy. Enriched with job SK and well EID. ~14K rows.
    columns:
      - name: record_id
        description: WellView interval problem natural key (IDRec)
        data_tests:
          - unique
          - not_null

      - name: job_sk
        description: FK to dim_job surrogate key
        data_tests:
          - relationships:
              arguments:
                to: ref('dim_job')
                field: job_sk
              config:
                severity: warn

      - name: eid
        description: Well entity identifier — FK to well_360
        data_tests:
          - relationships:
              arguments:
                to: ref('well_360')
                field: eid
              config:
                severity: warn

      - name: event_date
        description: Date the NPT event began (start_date cast to date)
        data_tests:
          - not_null:
              config:
                severity: warn

      - name: parent_record_id
        description: WellView job GUID (IDRECPARENT — natural FK to dim_job)
        data_tests:
          - not_null

      - name: well_id
        description: WellView well GUID
        data_tests:
          - not_null

      - name: problem_duration_gross_hours
        description: Total event duration from start to end (hours)
        data_tests:
          - not_null:
              config:
                severity: warn
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              config:
                severity: warn

      - name: problem_duration_net_hours
        description: Event duration after adjustments (hours)
        data_tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              config:
                severity: warn

      - name: major_category
        description: High-level problem grouping (e.g., rig equip, formation)
        data_tests:
          - not_null:
              config:
                severity: warn
```

---

## Task 4: Validation

### HOOEY N731H Ground Truth (EID `109181`)

#### fct_drilling_time

```sql
-- Expected: 574 entries, ~2,599 total hours, ~678.5 drilling hours / ~28.3 days
-- Should roughly match job duration (31 report days minus non-drilling time)
select
    time_log_code_1,
    count(*) as entries,
    round(sum(duration_hours), 1) as total_hours,
    round(sum(problem_time_hours), 1) as npt_hours
from {{ ref('fct_drilling_time') }}
where eid = '109181'
group by 1
order by 2 desc
```

#### fct_npt_events

```sql
-- Expected: 3 events, 9.0 gross hours, all "rig equip / Surface Equipment"
select
    major_category,
    problem_type,
    count(*) as events,
    round(sum(problem_duration_gross_hours), 1) as gross_hours,
    round(sum(problem_cost), 2) as total_cost
from {{ ref('fct_npt_events') }}
where eid = '109181'
group by 1, 2
```

#### FK Integrity Checks

```sql
-- Verify time log job_id matches dim_job
select
    count(*) as total,
    count(case when dj.job_sk is not null then 1 end) as matched,
    count(case when dj.job_sk is null then 1 end) as orphaned
from {{ ref('fct_drilling_time') }} ft
left join {{ ref('dim_job') }} dj on ft.job_sk = dj.job_sk

-- Verify NPT parent_record_id matches dim_job
select
    count(*) as total,
    count(case when dj.job_sk is not null then 1 end) as matched,
    count(case when dj.job_sk is null then 1 end) as orphaned
from {{ ref('fct_npt_events') }} npt
left join {{ ref('dim_job') }} dj on npt.job_sk = dj.job_sk

-- Check for negative durations in NPT (data quality)
select count(*) as negative_durations
from {{ ref('fct_npt_events') }}
where problem_duration_gross_hours < 0
   or problem_duration_net_hours < 0

-- Verify phase_sk matches dim_phase (cross-reference check)
select
    count(*) as total,
    count(case when dp.phase_sk is not null then 1 end) as matched,
    count(case when dp.phase_sk is null then 1 end) as orphaned
from {{ ref('fct_drilling_time') }} ft
left join {{ ref('dim_phase') }} dp on ft.phase_sk = dp.phase_sk

-- Check is_report_date_inferred rate (should be low)
select
    count(*) as total,
    count(case when is_report_date_inferred then 1 end) as inferred,
    round(100.0 * count(case when is_report_date_inferred then 1 end) / count(*), 1) as pct_inferred
from {{ ref('fct_drilling_time') }}
```

### Portfolio Validation

```sql
-- fct_drilling_time: expect ~762K rows, ~4,542 wells
select
    count(*) as total_rows,
    count(distinct well_id) as distinct_wells,
    count(distinct job_id) as distinct_jobs
from {{ ref('fct_drilling_time') }}

-- fct_npt_events: expect ~14K rows, ~1,429 wells
select
    count(*) as total_rows,
    count(distinct well_id) as distinct_wells,
    count(distinct parent_record_id) as distinct_jobs
from {{ ref('fct_npt_events') }}
```

---

## Execution Order

1. Write `fct_drilling_time.sql` in `models/operations/marts/drilling/`
2. Write `fct_npt_events.sql` in `models/operations/marts/drilling/`
3. Update `models/operations/marts/drilling/schema.yml` with both model definitions
4. Run `dbt build --select fct_drilling_time fct_npt_events` (builds both + tests)
5. Validate HOOEY N731H ground truth for both models
6. Validate portfolio row counts and FK integrity
7. Run `sqlfluff lint` on new SQL files
8. Run `yamllint` on updated YAML
9. Commit and push

## Acceptance Criteria

- [x] `fct_drilling_time` builds successfully with ~762K rows — **754,118 rows**
- [x] `fct_npt_events` builds successfully with ~14K rows — **14,013 rows**
- [x] PK uniqueness passes: `time_log_id` (unique + not_null), `record_id` (unique + not_null)
- [x] FK relationships pass (warn severity): `job_sk` → `dim_job`, `eid` → `well_360`
- [x] HOOEY validation: drilling time ≈ 574 entries / 2,599 hrs — **exact match**
- [x] HOOEY validation: NPT = 3 events / 9.0 gross hrs / "rig equip" — **exact match**
- [x] `phase_sk` FK to `dim_phase` passes (warn severity) — **217K matched, 537K orphaned (71% NULL phase IDs in source)**
- [x] `report_date` populated for time entries (warn-level not_null) — **353 NULLs (0.05%), 6.6% inferred**
- [x] `event_date` populated for NPT events (warn-level not_null) — **32 NULLs (0.2%)**
- [x] FK match rate: >95% of time log `job_sk` values exist in `dim_job` — **100% (0 orphans)**
- [x] FK match rate: >95% of NPT `job_sk` values exist in `dim_job` — **100% (0 orphans)**
- [x] No negative NPT durations (or documented if they exist) — **22 negative durations flagged by warn-level test**
- [x] `dbt show` spot-check: sample records have plausible depths, durations, and codes
- [x] SQLFluff lint passes
- [x] yamllint passes
- [ ] `dbt parse --warn-error --no-partial-parse` passes

## Risk Mitigations

- **parent_record_id mapping:** Verify with `dbt show` that NPT `parent_record_id` values exist in `stg_wellview__jobs.job_id`. If they don't match, NPT events may link through daily reports instead — add a join chain.
- **Zero-duration entries:** 8 entries in HOOEY have null `time_log_code_1` and 0 duration. These pass through; consumers filter as needed.
- **Table materialization growth:** 762K rows today. If time log data grows past ~5M rows, convert `fct_drilling_time` to incremental (merge on `time_log_id`, watermark on `_loaded_at`, cluster by `well_id, job_id`).

## References

- Brainstorm: `docs/brainstorms/2026-02-14-wellview-intermediate-mart-modeling-brainstorm.md` (Sprint 2 section)
- Sprint 1 plan: `docs/plans/2026-02-14-sprint-1-fct-daily-drilling-cost.md`
- Sprint 1 patterns: `docs/solutions/refactoring/drilling-mart-sprint-1-intermediate-patterns.md`
- Entity model: `context/sources/wellview/entity_model.md`
- Staging SQL: `models/operations/staging/wellview/operations/stg_wellview__job_time_log.sql`
- Staging SQL: `models/operations/staging/wellview/operations/stg_wellview__job_interval_problems.sql`
- Existing fact pattern: `models/operations/marts/drilling/fct_daily_drilling_cost.sql`
