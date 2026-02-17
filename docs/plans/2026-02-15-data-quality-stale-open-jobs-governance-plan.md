# Data Quality Governance: Stale Open Jobs

**Created:** 2026-02-15
**Status:** Planned (future sprint)
**Domain:** Drilling Marts / Data Governance
**Source System:** WellView

## Problem Statement

8% of WellView jobs (3,442 of 42,825) have no `job_end_at` date, causing `is_active = true` and inflated `duration_days` in `dim_job`. Two root causes:

1. **Acquired-well legacy data** — Jobs imported from prior operators that never had end dates populated (e.g., 263 "General Wellwork" jobs dating to 1955, 163 "Original Drilling" jobs from 1962). Many job types are 100% open.
2. **Close-out discipline gap** — Active Formentera wells where Completion, Facilities Initial Build, and Flowback jobs are not being closed after the well moves to production. Example: Hooey-STX-Unit N731H has been producing since August 2025 but still shows 3 open jobs.

### Affected Job Types (Close-Out Gap)

| Job Type | Total | Still Open | % Open |
|----------|------:|----------:|---------:|
| Facilites Initial Build | 8 | 8 | 100% |
| Original Completion | 41 | 16 | 39% |
| Flowback | 31 | 5 | 16% |

### Impact

- `is_active` flag is unreliable for these job types
- `duration_days` computes start-to-now instead of actual job duration
- Any downstream reporting on "active jobs" or "average job duration" is distorted
- No visibility for source system owners that jobs need closing

## Proposed Solution

Three-layer approach: detect, report, notify.

### Layer 1: dbt Singular Test (CI Visibility)

A `severity: warn` singular test that surfaces stale open jobs in CI and Elementary.

**File:** `tests/data_quality/stale_open_jobs.sql`

**Logic:**
- Job started 90+ days ago
- No `job_end_at` (is_active = true)
- No time-log activity in the last 30 days (cross-ref `fct_drilling_time`)
- Exclude known legacy job types that will never be closed (configurable list)

**Config:**
```yaml
data_tests:
  - stale_open_jobs:
      config:
        severity: warn
        tags: ['data_quality', 'governance']
```

**Acceptance criteria:**
- Test runs in CI without blocking builds
- Elementary picks up warnings in observability dashboard
- Legacy job types (acquired data) are excluded via a seed or variable

### Layer 2: Materialized Data Quality Report

A mart-layer model that materializes actionable findings for source system owners.

**File:** `models/operations/marts/data_quality/dq_stale_open_jobs.sql`
**Materialization:** `table`
**Refresh:** Every dbt build (lightweight query)

**Columns:**
- `well_name` (from well_360 or well_header for human readability)
- `eid`
- `job_id`, `job_type_primary`, `job_category`
- `job_start_at`
- `days_open` (datediff from start to now)
- `last_time_log_date` (most recent activity from `fct_drilling_time`)
- `last_cost_date` (most recent cost entry from `fct_daily_drilling_cost`)
- `days_since_last_activity`
- `stale_category` (enum: `legacy_acquisition`, `close_out_gap`, `possibly_active`)
- `recommended_action` (text: "Close job in WellView" / "Review — may still be active" / "Legacy data — bulk close")

**Stale classification logic:**
- `legacy_acquisition`: job_type_primary in known legacy list OR job_start_at < acquisition cutoff dates
- `close_out_gap`: Formentera-era job with no activity in 30+ days
- `possibly_active`: Formentera-era job with recent activity (included for completeness but not flagged)

### Layer 3: Snowflake Scheduled Email Notification

Weekly email digest to WellView administrators with top-line numbers and actionable items.

**Infrastructure:**
1. Snowflake Notification Integration (email type)
2. Snowflake Task on weekly cron schedule (Monday 8am CT)
3. Stored procedure that queries `dq_stale_open_jobs` and formats email body

**Email content:**
- Subject: `WellView Data Quality: {N} Jobs Need Close-Out`
- Summary counts by stale_category
- Top 10 most egregious (longest days_open with close_out_gap category)
- Link to full report (Snowflake worksheet or Elementary dashboard)

**Recipients:** WellView admin distribution list (TBD with ops team)

**Setup requirements:**
- `ACCOUNTADMIN` to create notification integration (one-time)
- `DBT_ROLE` needs `EXECUTE TASK` privilege
- Email recipients must be registered in Snowflake notification integration

## Implementation Plan

### Sprint Tasks

1. **Identify legacy job type exclusion list** — Query dim_job to build the list of job_type_primary values that are 100% open and clearly legacy. Materialize as a seed CSV.
2. **Build singular test** (`stale_open_jobs.sql`) — Implement detection logic with seed-based exclusions. Run `dbt test` to validate.
3. **Build DQ report model** (`dq_stale_open_jobs.sql`) — Materialize findings table with classification logic and recommended actions.
4. **Add YAML schema** — Document the DQ model with column descriptions and tests (unique on job_id, not_null on stale_category).
5. **Set up Snowflake email integration** — Create notification integration and scheduled task. Requires Snowflake admin access.
6. **Validate with ops team** — Review the report output with WellView admins, confirm recipients, adjust thresholds.

### Dependencies

- `dim_job` (Sprint 1 — exists)
- `fct_drilling_time` (Sprint 2 — exists)
- `fct_daily_drilling_cost` (Sprint 1 — exists)
- `well_360` or `stg_wellview__well_header` (for well_name resolution — exists)
- Snowflake admin access for notification integration setup

### Estimated Effort

| Task | Estimate |
|------|----------|
| Legacy exclusion seed | Small |
| Singular test | Small |
| DQ report model + YAML | Medium |
| Snowflake email setup | Medium (admin coordination) |
| Ops team review | Meeting |
| **Total** | **~1 sprint** |

## Future Considerations

- **Expand pattern to other source systems** — ODA GL entries with stale batches, ProdView allocation gaps
- **Elementary alerts** — If Slack integration is set up, add Elementary alert on the warn-severity test for real-time visibility
- **Automated close-out** — If ops team agrees on rules (e.g., "close Flowback jobs 30 days after last activity"), could build a WellView API integration to auto-close. High risk, needs careful scoping.
- **DQ dashboard** — Aggregate all data quality findings across domains into a single Elementary or Snowflake dashboard for leadership visibility
