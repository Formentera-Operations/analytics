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
    select
        wellview_id,
        eid
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
