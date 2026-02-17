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
        (p.actual_start_date is not null) as is_realized,

        -- Audit
        current_timestamp() as _loaded_at

    from phases as p
    left join jobs as j
        on p.parent_record_id = j.job_id
)

select * from joined
