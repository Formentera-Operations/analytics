{{
    config(
        enabled=true,
        materialized='view'
    )
}}

with jobs as (
    select *
    from {{ ref('stg_wellview__jobs') }}
),

wells as (
    select *
    from {{ ref('stg_wellview__well_header') }}
)

select
    -- identifiers
    jobs.job_id,
    jobs.well_id,
    jobs.wellbore_id,

    -- job classification
    jobs.job_category,
    jobs.job_type_primary,
    jobs.job_type_secondary,

    -- key dates
    jobs.job_start_at,
    jobs.job_end_at,
    jobs.calculated_end_at,
    jobs.planned_start_at,
    jobs.spud_at,

    -- duration
    jobs.duration_start_to_end_days,

    -- depths (ft)
    jobs.target_depth_ft,
    jobs.total_depth_ft,
    jobs.depth_drilled_ft,

    -- drilling performance
    jobs.rop_ft_per_hr,
    jobs.drilling_time_hours,
    jobs.time_log_total_hours,

    -- afe and cost
    jobs.afe_number,
    jobs.afe_amount,
    jobs.afe_cost_type,
    jobs.afe_total_amount,
    jobs.total_field_estimate,
    jobs.cost_forecast,

    -- status
    jobs.status_primary,
    jobs.status_secondary,
    jobs.technical_result,

    -- objectives
    jobs.job_objective,
    jobs.job_summary,

    -- well enrichment
    wells.cost_center as well_code,
    wells.well_type,
    wells.api_10_number,

    -- dbt metadata
    jobs._loaded_at

from jobs
left join wells
    on jobs.well_id = wells.well_id
