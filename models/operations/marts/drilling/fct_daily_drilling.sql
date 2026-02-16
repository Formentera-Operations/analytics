{{
    config(
        materialized='table',
        cluster_by=['job_id', 'report_date'],
        tags=['drilling', 'mart', 'fact']
    )
}}

with source as (
    select * from {{ ref('int_wellview__daily_drilling_enriched') }}
),

final as (
    select
        daily_drilling_sk,

        -- dimensional FKs
        job_sk,
        wellbore_sk,
        eid,

        -- natural keys
        report_id,
        job_id,
        well_id,
        wellbore_id,

        -- temporal
        report_date,
        report_number,
        days_from_spud,

        -- report-level drilling metrics
        start_depth_ft,
        end_depth_ft,
        depth_progress_ft,
        net_depth_progress_ft,
        rop_ft_per_hr,
        drilling_time_hours,
        rotating_time_hours,
        sliding_time_hours,
        circulating_time_hours,
        tripping_time_hours,
        other_time_hours,
        total_time_log_hours,
        problem_time_hours,
        no_problem_time_hours,
        rig_days,
        cumulative_rig_days,

        -- child rollups
        cost_line_count,
        daily_field_estimate_cost,
        time_log_entry_count,
        daily_time_duration_hours,
        daily_time_problem_hours,
        daily_time_no_problem_hours,
        daily_on_bottom_minutes,
        daily_off_bottom_minutes,
        daily_pipe_moving_minutes,
        npt_event_count,
        daily_npt_gross_hours,
        daily_npt_net_hours,
        daily_npt_cost,
        npt_join_used_date_fallback,
        safety_event_count,
        safety_check_count,
        safety_incident_count,
        safety_lost_time_hours,
        safety_estimated_cost,
        safety_join_used_date_fallback,

        -- source freshness watermark
        source_synced_at,

        -- dbt metadata
        _loaded_at
    from source
)

select * from final
