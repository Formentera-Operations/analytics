# Sprint 1 Implementation Script: Core Drilling Dimensions

## Objective

Build the dimensional backbone for drilling operations analytics. Four new models + one deletion. No new staging models needed — everything sources from existing staging views.

## Branch

`feature/sprint-1-drilling-dimensions`

## File Plan

```
models/operations/marts/drilling/           # NEW directory
├── dim_job.sql                             # Task 1
├── dim_wellbore.sql                        # Task 2
├── dim_phase.sql                           # Task 3
├── bridge_job_afe.sql                      # Task 4
└── schema.yml                              # Tests + docs for all 4 models
models/operations/marts/fct_job_performance.sql  # DELETE (Task 5)
```

**dbt_project.yml routing** — add under `models:` config:

```yaml
drilling:
  +materialized: table
  +tags: ['drilling', 'mart']
  +schema: drilling
```

---

## Task 1: `dim_job` — Job Dimension

**Grain:** One row per WellView job (no time filter — dimensions are complete).

**Sources:**

| Model | Join Key | Purpose |
|-------|----------|---------|
| `stg_wellview__jobs` | — (base) | Job attributes |
| `stg_wellview__well_header` | `"Well ID"` | Get EID for well_360 FK |
| `stg_wellview__rigs` | `job_id = "Job ID"` | Latest rig per job |
| `well_360` | `eid` | Validate well exists in dimension |

**Column naming challenge:** `stg_wellview__jobs` uses quoted names with spaces (`"Job ID"`, `"Primary Job Type"`). All other staging models use snake_case. The dimension must output snake_case.

### SQL Pattern

```sql
{{
    config(
        materialized='table',
        tags=['drilling', 'mart', 'dimension']
    )
}}

with jobs as (
    select * from {{ ref('stg_wellview__jobs') }}
),

well_header as (
    select
        "Well ID" as well_id,
        "EID" as eid
    from {{ ref('stg_wellview__well_header') }}
    where "EID" is not null
      and len("EID") = 6
),

wells as (
    select eid from {{ ref('well_360') }}
),

-- Latest rig per job (by end date, then start date, then ID)
rigs as (
    select *
    from {{ ref('stg_wellview__rigs') }}
    qualify row_number() over (
        partition by job_id
        order by
            rig_end_datetime desc nulls last,
            rig_start_datetime desc nulls last,
            job_rig_id desc
    ) = 1
),

joined as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['j."Job ID"']) }} as job_sk,

        -- Well FK (through EID → well_360)
        w.eid,

        -- Natural key
        j."Job ID" as job_id,
        j."Well ID" as well_id,
        j."Wellbore ID" as wellbore_id,

        -- Job classification
        j."Primary Job Type" as job_type_primary,
        j."Secondary Job Type" as job_type_secondary,
        j."Wellview Job Category" as job_category,
        j."Complexity Index" as complexity_index,
        j."Job Objective" as job_objective,
        j."Geological Objective" as geological_objective,
        j."Target Formation" as target_formation,

        -- Key dates
        j."Job Start Datetime" as job_start_at,
        j."Job End Datetime" as job_end_at,
        j."Spud Datetime" as spud_at,
        j."Planned Start Datetime" as planned_start_at,
        j."Calculated End Datetime" as calculated_end_at,

        -- Duration
        j."Duration Start To End Days" as duration_days,

        -- Depths (already in FT from staging)
        j."Target Depth Ft" as target_depth_ft,
        j."Total Depth Reached Ft" as total_depth_ft,
        j."Depth Drilled Ft" as depth_drilled_ft,

        -- Drilling performance (already converted in staging)
        j."Rop Ft Per hr" as rop_ft_per_hr,
        j."Drilling Time Hours" as drilling_time_hours,
        j."Rotating Time Hours" as rotating_time_hours,
        j."Sliding Time Hours" as sliding_time_hours,
        j."Tripping Time Hours" as tripping_time_hours,
        j."Circulating Time Hours" as circulating_time_hours,

        -- Cost summary
        j."AFE Number" as afe_number_primary,
        j."AFE Amount" as afe_amount_primary,
        j."Total Field Estimate" as total_field_estimate,

        -- Status
        j."Primary Status" as status_primary,
        j."Secondary Status" as status_secondary,
        j."Technical Result" as technical_result,

        -- Rig enrichment (latest rig)
        r.rig_contractor,
        r.rig_number,
        r.rig_type,
        r.rig_category,
        r.rig_start_datetime as rig_start_at,
        r.rig_end_datetime as rig_end_at,

        -- Flags
        case
            when j."Job End Datetime" is null then true
            else false
        end as is_active,

        -- Audit
        current_timestamp() as _loaded_at

    from jobs as j
    left join well_header as wh
        on j."Well ID" = wh.well_id
    left join wells as w
        on wh.eid = w.eid
    left join rigs as r
        on j."Job ID" = r.job_id
),

final as (
    select * from joined
)

select * from final
```

### Tests (schema.yml)

- `job_sk`: unique, not_null
- `job_id`: unique, not_null
- `eid`: relationships to `well_360`
- `job_type_primary`: not_null

### Validation

```bash
dbt build --select dim_job
dbt show --select dim_job --limit 10
# Check: row count matches stg_wellview__jobs
# Check: eid NOT NULL rate (expect >95%, some legacy jobs may lack well mapping)
# Check: rig_contractor populated for most jobs
```

---

## Task 2: `dim_wellbore` — Wellbore Dimension

**Grain:** One row per wellbore.

**Sources:**

| Model | Join Key | Purpose |
|-------|----------|---------|
| `stg_wellview__wellbores` | — (base) | Wellbore attributes |
| `stg_wellview__well_header` | `well_id` | Get EID |
| `well_360` | `eid` | Well FK |

**Note:** `stg_wellview__wellbore_depths` has multiple rows per wellbore (one per key depth type). For the dimension, we pull specific depth types (TD, KOP, landing) as pivoted columns rather than joining the full grain.

### SQL Pattern

```sql
{{
    config(
        materialized='table',
        tags=['drilling', 'mart', 'dimension']
    )
}}

with wellbores as (
    select * from {{ ref('stg_wellview__wellbores') }}
),

well_header as (
    select
        "Well ID" as well_id,
        "EID" as eid
    from {{ ref('stg_wellview__well_header') }}
    where "EID" is not null
      and len("EID") = 6
),

wells as (
    select eid from {{ ref('well_360') }}
),

-- Pivot specific key depths per wellbore from the key depths table
-- Only if we need depths beyond what wellbores already has
-- (wellbores already has total_depth_ft, total_depth_tvd_ft, min_kickoff_depth_ft)

joined as (
    select
        {{ dbt_utils.generate_surrogate_key(['wb.record_id']) }} as wellbore_sk,

        -- Well FK
        w.eid,

        -- Natural keys
        wb.record_id as wellbore_id,
        wb.well_id,
        wb.parent_wellbore_id,

        -- Identification
        wb.wellbore_name,
        wb.wellbore_api_uwi,
        wb.purpose,
        wb.profile_type,
        wb.current_status,
        wb.current_status_date,

        -- Depths (already in FT from staging)
        wb.start_depth_ft,
        wb.total_depth_ft as md_total_ft,
        wb.total_depth_tvd_ft as tvd_total_ft,
        wb.min_kickoff_depth_ft as kickoff_depth_ft,
        wb.draw_depth_ft,

        -- Directional
        wb.max_inclination_degrees,
        wb.max_dls_degrees_per_100ft,
        wb.max_departure_ft,
        wb.unwrapped_displacement_ft,
        wb.closure_direction_degrees,

        -- Dates
        wb.start_drill_date as spud_date,
        wb.end_drill_date as td_date,
        wb.duration_hours,

        -- Location (bottom hole)
        wb.latitude_degrees,
        wb.longitude_degrees,

        -- Flags
        case
            when wb.profile_type ilike '%horizontal%' then 'Horizontal'
            when wb.profile_type ilike '%directional%' then 'Directional'
            when wb.profile_type ilike '%vertical%' then 'Vertical'
            else wb.profile_type
        end as profile_category,

        -- Audit
        current_timestamp() as _loaded_at

    from wellbores as wb
    left join well_header as wh
        on wb.well_id = wh.well_id
    left join wells as w
        on wh.eid = w.eid
),

final as (
    select * from joined
)

select * from final
```

### Tests

- `wellbore_sk`: unique, not_null
- `wellbore_id`: unique, not_null
- `eid`: relationships to `well_360`

### Validation

```bash
dbt build --select dim_wellbore
dbt show --select dim_wellbore --limit 10
# Check: row count matches stg_wellview__wellbores
# Check: profile_category distribution (expect mostly Horizontal for Formentera)
# Check: md_total_ft reasonable range (5,000-25,000 ft typical)
```

---

## Task 3: `dim_phase` — Phase Dimension

**Grain:** One row per job program phase.

**Sources:**

| Model | Join Key | Purpose |
|-------|----------|---------|
| `stg_wellview__job_program_phases` | — (base) | Phase attributes |
| `dim_job` | `parent_record_id = job_id` | Job FK |

### SQL Pattern

```sql
{{
    config(
        materialized='table',
        tags=['drilling', 'mart', 'dimension']
    )
}}

with phases as (
    select * from {{ ref('stg_wellview__job_program_phases') }}
),

jobs as (
    select
        job_sk,
        job_id,
        eid
    from {{ ref('dim_job') }}
),

joined as (
    select
        {{ dbt_utils.generate_surrogate_key(['p.record_id']) }} as phase_sk,

        -- FKs
        j.job_sk,
        j.eid,

        -- Natural keys
        p.record_id as phase_id,
        p.parent_record_id as job_id,
        p.well_id,
        p.wellbore_id,

        -- Phase classification
        p.phase_type_1,
        p.phase_type_2,
        p.phase_type_3,
        p.phase_type_4,
        p.combined_phase_types,
        p.description as phase_description,

        -- Planned vs Actual dates
        p.actual_start_date,
        p.actual_end_date,
        p.derived_end_date,

        -- Planned vs Actual depths (FT)
        p.planned_start_depth_ft,
        p.planned_end_depth_ft,
        p.actual_start_depth_ft,
        p.actual_end_depth_ft,
        p.planned_depth_progress_ft,
        p.actual_depth_progress_ft,

        -- Duration (days)
        p.planned_likely_duration_days,
        p.planned_min_duration_days,
        p.planned_max_duration_days,
        p.actual_duration_days,
        p.duration_variance_days,

        -- Drilling performance (hours, already converted)
        p.drilling_time_hours,
        p.rotating_time_hours,
        p.sliding_time_hours,
        p.circulating_time_hours,
        p.tripping_time_hours,
        p.problem_time_hours,
        p.time_log_total_hours,

        -- ROP (FT/HR)
        p.rop_ft_per_hour,

        -- Cost
        p.actual_phase_field_est,
        p.planned_likely_phase_cost,

        -- Flags
        p.is_plan_change,
        p.exclude_from_calculations,
        case
            when p.actual_start_date is not null then true
            else false
        end as is_realized,

        -- Audit
        current_timestamp() as _loaded_at

    from phases as p
    left join jobs as j
        on p.parent_record_id = j.job_id
),

final as (
    select * from joined
)

select * from final
```

### Tests

- `phase_sk`: unique, not_null
- `phase_id`: unique, not_null
- `job_sk`: relationships to `dim_job`

### Validation

```bash
dbt build --select dim_phase
dbt show --select dim_phase --limit 10
# Check: row count matches stg_wellview__job_program_phases
# Check: job_sk NOT NULL rate (expect high — orphan phases are a data quality signal)
# Check: is_realized distribution (phases with actual_start_date)
```

---

## Task 4: `bridge_job_afe` — Job-AFE Bridge

**Grain:** One row per AFE record linked to a job. Many-to-many: a job can have multiple AFEs, an AFE can fund multiple jobs.

**Sources:**

| Model | Join Key | Purpose |
|-------|----------|---------|
| `stg_wellview__job_afe_definitions` | — (base) | AFE records |
| `dim_job` | `job_id` | Job FK |

### SQL Pattern

```sql
{{
    config(
        materialized='table',
        tags=['drilling', 'mart', 'bridge']
    )
}}

with afe_defs as (
    select * from {{ ref('stg_wellview__job_afe_definitions') }}
),

jobs as (
    select
        job_sk,
        job_id
    from {{ ref('dim_job') }}
),

joined as (
    select
        {{ dbt_utils.generate_surrogate_key(['a.job_afe_id']) }} as job_afe_sk,

        -- FKs
        j.job_sk,

        -- Natural keys
        a.job_afe_id,
        a.job_id,
        a.well_id,

        -- AFE identification
        a.afe_number,
        a.supplemental_afe_number,
        a.afe_type,
        a.afe_cost_type,
        a.afe_status,
        a.cost_type,

        -- Dates
        a.afe_date,
        a.afe_close_date,

        -- Working interest
        a.working_interest_percent,

        -- AFE amounts (gross — primary for drilling ops)
        a.total_afe_amount,
        a.total_afe_supplemental_amount,
        a.total_afe_plus_supplemental_amount,

        -- Field estimates
        a.total_field_estimate,

        -- Variances
        a.afe_minus_field_estimate_variance,

        -- Flags
        a.exclude_from_cost_calculations,

        -- Audit
        current_timestamp() as _loaded_at

    from afe_defs as a
    left join jobs as j
        on a.job_id = j.job_id
),

final as (
    select * from joined
)

select * from final
```

### Tests

- `job_afe_sk`: unique, not_null
- `job_afe_id`: unique, not_null
- `job_sk`: relationships to `dim_job`
- `afe_number`: not_null

### Validation

```bash
dbt build --select bridge_job_afe
dbt show --select bridge_job_afe --limit 10
# Check: multiple AFEs per job exist (confirms M:M)
# Check: job_sk NOT NULL rate
# Check: afe_number format consistency
```

---

## Task 5: Delete `fct_job_performance`

The disabled model at `models/operations/marts/fct_job_performance.sql` is superseded by this sprint's dimensions (and Sprint 2's fact tables).

```bash
git rm models/operations/marts/fct_job_performance.sql
```

Also remove any references in schema YAML files.

---

## dbt_project.yml Update

Add routing for the new `drilling/` subdomain:

```yaml
# Under models > formentera_analytics > operations > marts:
drilling:
  +materialized: table
  +tags: ['drilling', 'mart']
```

---

## Build Order

Models must be built in dependency order:

```
1. dim_job        (no mart dependencies — sources from staging only)
2. dim_wellbore   (no mart dependencies — sources from staging only)
3. dim_phase      (depends on dim_job)
4. bridge_job_afe (depends on dim_job)
```

Tasks 1 and 2 can be built in parallel. Tasks 3 and 4 depend on Task 1.

### Full build command

```bash
dbt build --select +dim_job +dim_wellbore +dim_phase +bridge_job_afe
```

---

## Acceptance Criteria

| Check | Expected |
|-------|----------|
| `dbt build --select` passes for all 4 models | Zero errors |
| Each SK column is unique + not_null | Tests pass |
| FK relationships resolve | Tests pass |
| `dim_job` row count ≈ `stg_wellview__jobs` | Within 1% |
| `dim_wellbore` row count ≈ `stg_wellview__wellbores` | Within 1% |
| `dim_phase` row count ≈ `stg_wellview__job_program_phases` | Within 1% |
| `bridge_job_afe` row count ≈ `stg_wellview__job_afe_definitions` | Within 1% |
| `fct_job_performance.sql` deleted | File gone, no parse errors |
| `sqlfluff lint` passes on all new models | Zero violations |
| `yamllint` passes on schema.yml | Zero violations |

---

## Key Design Decisions

1. **Snake_case output** — All dimension columns use snake_case even though `stg_wellview__jobs` uses quoted names. The alias in the SELECT handles the conversion.

2. **EID as well FK** — Join through `stg_wellview__well_header` to get EID, then validate against `well_360`. This keeps `well_360` as the authoritative well dimension without duplicating its logic.

3. **Surrogate keys on natural keys** — `generate_surrogate_key(['record_id'])` wraps the WellView IDRec. This gives us a stable hash-based SK that's consistent across rebuilds.

4. **No time filters on dimensions** — Unlike `fct_eng_jobs` (3-year filter), dimensions are complete. Historical jobs matter for trend analysis.

5. **Rig enrichment via qualify** — Latest rig per job using `qualify row_number()` rather than creating an intermediate model. Single-use transformation stays in the dimension.

6. **Phase → Job dependency** — `dim_phase` depends on `dim_job` for `job_sk`. This is intentional: the phase dimension carries its parent's SK for easy downstream joins.

7. **Bridge amounts are gross** — Using gross AFE amounts (not normalized/net) as the primary financial column. Normalized amounts are a future enhancement if multi-currency becomes relevant.

## Column Naming Convention Reference

| Source Model | Column Style | Example |
|---|---|---|
| `stg_wellview__jobs` | Quoted with spaces | `"Job ID"`, `"Primary Job Type"` |
| `stg_wellview__well_header` | Quoted with spaces | `"Well ID"`, `"EID"` |
| `stg_wellview__wellbores` | snake_case | `record_id`, `well_id` |
| `stg_wellview__job_program_phases` | snake_case | `record_id`, `phase_type_1` |
| `stg_wellview__job_afe_definitions` | snake_case | `job_afe_id`, `afe_number` |
| `stg_wellview__rigs` | snake_case | `job_rig_id`, `rig_contractor` |
| **All new marts** | **snake_case** | `job_sk`, `job_type_primary` |
