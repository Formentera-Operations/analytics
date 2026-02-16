-- Enriches WellView daily reports with rolled-up cost, time, NPT, and safety measures.
-- Grain: 1 row per daily report (stg_wellview__job_reports.report_id).
-- Ephemeral — compiles as CTE into fct_daily_drilling.

with job_reports as (
    select
        report_id,
        job_id,
        well_id,
        wellbore_id,
        report_start_datetime::date as report_date,
        report_number,
        days_from_spud,
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
        _fivetran_synced,
        _loaded_at
    from {{ ref('stg_wellview__job_reports') }}
),

job_reports_ranked as (
    select
        jr.*,
        row_number() over (
            partition by jr.job_id, jr.report_date
            order by coalesce(jr.report_number, -1) desc, jr.report_id desc
        ) as report_date_rank
    from job_reports as jr
),

daily_costs_agg as (
    select
        job_report_id,
        count(*) as cost_line_count,
        sum(coalesce(field_estimate_cost, 0)) as daily_field_estimate_cost,
        max(_fivetran_synced) as max_cost_synced_at
    from {{ ref('stg_wellview__daily_costs') }}
    group by job_report_id
),

time_logs_filtered as (
    -- Exclude extreme duration outliers (source artifacts) from daily attribution.
    -- Profiled distribution shows p99/p999 abs(duration_hours)=24.
    select *
    from {{ ref('stg_wellview__job_time_log') }}
    where
        not coalesce(is_inactive, false)
        and abs(coalesce(duration_hours, 0)) <= 24
),

time_log_by_report_id as (
    select
        job_report_id,
        count(*) as time_log_entry_count,
        sum(coalesce(duration_hours, 0)) as daily_time_duration_hours,
        sum(coalesce(problem_time_hours, 0)) as daily_time_problem_hours,
        sum(coalesce(no_problem_time_hours, 0)) as daily_time_no_problem_hours,
        sum(coalesce(on_bottom_duration_minutes, 0)) as daily_on_bottom_minutes,
        sum(coalesce(off_bottom_duration_minutes, 0)) as daily_off_bottom_minutes,
        sum(coalesce(pipe_moving_duration_minutes, 0)) as daily_pipe_moving_minutes,
        max(_fivetran_synced) as max_time_log_synced_at
    from time_logs_filtered
    where job_report_id is not null
    group by job_report_id
),

time_log_by_report_number as (
    select
        job_id,
        report_number,
        count(*) as time_log_entry_count,
        sum(coalesce(duration_hours, 0)) as daily_time_duration_hours,
        sum(coalesce(problem_time_hours, 0)) as daily_time_problem_hours,
        sum(coalesce(no_problem_time_hours, 0)) as daily_time_no_problem_hours,
        sum(coalesce(on_bottom_duration_minutes, 0)) as daily_on_bottom_minutes,
        sum(coalesce(off_bottom_duration_minutes, 0)) as daily_off_bottom_minutes,
        sum(coalesce(pipe_moving_duration_minutes, 0)) as daily_pipe_moving_minutes,
        max(_fivetran_synced) as max_time_log_synced_at
    from time_logs_filtered
    where
        job_report_id is null
        and report_number is not null
    group by job_id, report_number
),

time_log_by_report_date as (
    select
        job_id,
        start_datetime::date as event_date,
        count(*) as time_log_entry_count,
        sum(coalesce(duration_hours, 0)) as daily_time_duration_hours,
        sum(coalesce(problem_time_hours, 0)) as daily_time_problem_hours,
        sum(coalesce(no_problem_time_hours, 0)) as daily_time_no_problem_hours,
        sum(coalesce(on_bottom_duration_minutes, 0)) as daily_on_bottom_minutes,
        sum(coalesce(off_bottom_duration_minutes, 0)) as daily_off_bottom_minutes,
        sum(coalesce(pipe_moving_duration_minutes, 0)) as daily_pipe_moving_minutes,
        max(_fivetran_synced) as max_time_log_synced_at
    from time_logs_filtered
    where
        job_report_id is null
        and report_number is null
        and start_datetime is not null
    group by job_id, start_datetime::date
),

npt_events as (
    select
        parent_record_id as job_id,
        report_number,
        start_date::date as event_date,
        problem_duration_gross_hours,
        problem_duration_net_hours,
        problem_cost,
        _fivetran_synced
    from {{ ref('stg_wellview__job_interval_problems') }}
),

npt_by_report_number as (
    select
        job_id,
        report_number,
        count(*) as npt_event_count,
        sum(coalesce(problem_duration_gross_hours, 0)) as daily_npt_gross_hours,
        sum(coalesce(problem_duration_net_hours, 0)) as daily_npt_net_hours,
        sum(coalesce(problem_cost, 0)) as daily_npt_cost,
        max(_fivetran_synced) as max_npt_synced_at
    from npt_events
    where report_number is not null
    group by job_id, report_number
),

npt_by_report_date as (
    select
        job_id,
        event_date,
        count(*) as npt_event_count,
        sum(coalesce(problem_duration_gross_hours, 0)) as daily_npt_gross_hours,
        sum(coalesce(problem_duration_net_hours, 0)) as daily_npt_net_hours,
        sum(coalesce(problem_cost, 0)) as daily_npt_cost,
        max(_fivetran_synced) as max_npt_synced_at
    from npt_events
    where report_number is null
    group by job_id, event_date
),

safety_checks as (
    select
        parent_record_id as job_id,
        report_number,
        check_datetime::date as event_date,
        1 as check_count,
        0 as incident_count,
        0::float as lost_time_hours,
        0::float as estimated_cost,
        _fivetran_synced
    from {{ ref('stg_wellview__safety_checks') }}
),

safety_incidents as (
    select
        parent_record_id as job_id,
        report_number,
        incident_datetime::date as event_date,
        0 as check_count,
        1 as incident_count,
        coalesce(lost_time_hours, 0) as lost_time_hours,
        coalesce(estimated_cost, 0) as estimated_cost,
        _fivetran_synced
    from {{ ref('stg_wellview__safety_incidents') }}
),

safety_events as (
    select * from safety_checks
    union all
    select * from safety_incidents
),

safety_by_report_number as (
    select
        job_id,
        report_number,
        sum(check_count + incident_count) as safety_event_count,
        sum(check_count) as safety_check_count,
        sum(incident_count) as safety_incident_count,
        sum(coalesce(lost_time_hours, 0)) as safety_lost_time_hours,
        sum(coalesce(estimated_cost, 0)) as safety_estimated_cost,
        max(_fivetran_synced) as max_safety_synced_at
    from safety_events
    where report_number is not null
    group by job_id, report_number
),

safety_by_report_date as (
    select
        job_id,
        event_date,
        sum(check_count + incident_count) as safety_event_count,
        sum(check_count) as safety_check_count,
        sum(incident_count) as safety_incident_count,
        sum(coalesce(lost_time_hours, 0)) as safety_lost_time_hours,
        sum(coalesce(estimated_cost, 0)) as safety_estimated_cost,
        max(_fivetran_synced) as max_safety_synced_at
    from safety_events
    where report_number is null
    group by job_id, event_date
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
        {{ dbt_utils.generate_surrogate_key(['jr.report_id']) }} as daily_drilling_sk,
        {{ dbt_utils.generate_surrogate_key(['jr.job_id']) }} as job_sk,
        case
            when jr.wellbore_id is not null then {{ dbt_utils.generate_surrogate_key(['jr.wellbore_id']) }}
        end as wellbore_sk,
        w360.eid,

        jr.report_id,
        jr.job_id,
        jr.well_id,
        jr.wellbore_id,
        jr.report_date,
        jr.report_number,
        jr.days_from_spud,

        jr.start_depth_ft,
        jr.end_depth_ft,
        jr.depth_progress_ft,
        jr.net_depth_progress_ft,
        jr.rop_ft_per_hr,
        jr.drilling_time_hours,
        jr.rotating_time_hours,
        jr.sliding_time_hours,
        jr.circulating_time_hours,
        jr.tripping_time_hours,
        jr.other_time_hours,
        jr.total_time_log_hours,
        jr.problem_time_hours,
        jr.no_problem_time_hours,
        jr.rig_days,
        jr.cumulative_rig_days,

        coalesce(dc.cost_line_count, 0) as cost_line_count,
        coalesce(dc.daily_field_estimate_cost, 0) as daily_field_estimate_cost,

        (
            coalesce(tl_id.time_log_entry_count, 0)
            + coalesce(tl_rn.time_log_entry_count, 0)
            + coalesce(tl_dt.time_log_entry_count, 0)
        ) as time_log_entry_count,
        (
            coalesce(tl_id.daily_time_duration_hours, 0)
            + coalesce(tl_rn.daily_time_duration_hours, 0)
            + coalesce(tl_dt.daily_time_duration_hours, 0)
        ) as daily_time_duration_hours,
        (
            coalesce(tl_id.daily_time_problem_hours, 0)
            + coalesce(tl_rn.daily_time_problem_hours, 0)
            + coalesce(tl_dt.daily_time_problem_hours, 0)
        ) as daily_time_problem_hours,
        (
            coalesce(tl_id.daily_time_no_problem_hours, 0)
            + coalesce(tl_rn.daily_time_no_problem_hours, 0)
            + coalesce(tl_dt.daily_time_no_problem_hours, 0)
        ) as daily_time_no_problem_hours,
        (
            coalesce(tl_id.daily_on_bottom_minutes, 0)
            + coalesce(tl_rn.daily_on_bottom_minutes, 0)
            + coalesce(tl_dt.daily_on_bottom_minutes, 0)
        ) as daily_on_bottom_minutes,
        (
            coalesce(tl_id.daily_off_bottom_minutes, 0)
            + coalesce(tl_rn.daily_off_bottom_minutes, 0)
            + coalesce(tl_dt.daily_off_bottom_minutes, 0)
        ) as daily_off_bottom_minutes,
        (
            coalesce(tl_id.daily_pipe_moving_minutes, 0)
            + coalesce(tl_rn.daily_pipe_moving_minutes, 0)
            + coalesce(tl_dt.daily_pipe_moving_minutes, 0)
        ) as daily_pipe_moving_minutes,
        (tl_rn.job_id is not null) as time_join_used_report_number_fallback,
        (tl_dt.job_id is not null) as time_join_used_date_fallback,

        coalesce(npt_rn.npt_event_count, 0) + coalesce(npt_dt.npt_event_count, 0) as npt_event_count,
        coalesce(npt_rn.daily_npt_gross_hours, 0) + coalesce(npt_dt.daily_npt_gross_hours, 0) as daily_npt_gross_hours,
        coalesce(npt_rn.daily_npt_net_hours, 0) + coalesce(npt_dt.daily_npt_net_hours, 0) as daily_npt_net_hours,
        coalesce(npt_rn.daily_npt_cost, 0) + coalesce(npt_dt.daily_npt_cost, 0) as daily_npt_cost,
        (npt_dt.job_id is not null) as npt_join_used_date_fallback,

        coalesce(safety_rn.safety_event_count, 0) + coalesce(safety_dt.safety_event_count, 0) as safety_event_count,
        coalesce(safety_rn.safety_check_count, 0) + coalesce(safety_dt.safety_check_count, 0) as safety_check_count,
        (
            coalesce(safety_rn.safety_incident_count, 0)
            + coalesce(safety_dt.safety_incident_count, 0)
        ) as safety_incident_count,
        (
            coalesce(safety_rn.safety_lost_time_hours, 0)
            + coalesce(safety_dt.safety_lost_time_hours, 0)
        ) as safety_lost_time_hours,
        (
            coalesce(safety_rn.safety_estimated_cost, 0)
            + coalesce(safety_dt.safety_estimated_cost, 0)
        ) as safety_estimated_cost,
        (safety_dt.job_id is not null) as safety_join_used_date_fallback,

        greatest(
            coalesce(jr._fivetran_synced::timestamp_ntz, '1900-01-01'::timestamp_ntz),
            coalesce(dc.max_cost_synced_at::timestamp_ntz, '1900-01-01'::timestamp_ntz),
            coalesce(tl_id.max_time_log_synced_at::timestamp_ntz, '1900-01-01'::timestamp_ntz),
            coalesce(tl_rn.max_time_log_synced_at::timestamp_ntz, '1900-01-01'::timestamp_ntz),
            coalesce(tl_dt.max_time_log_synced_at::timestamp_ntz, '1900-01-01'::timestamp_ntz),
            coalesce(npt_rn.max_npt_synced_at::timestamp_ntz, '1900-01-01'::timestamp_ntz),
            coalesce(npt_dt.max_npt_synced_at::timestamp_ntz, '1900-01-01'::timestamp_ntz),
            coalesce(safety_rn.max_safety_synced_at::timestamp_ntz, '1900-01-01'::timestamp_ntz),
            coalesce(safety_dt.max_safety_synced_at::timestamp_ntz, '1900-01-01'::timestamp_ntz)
        ) as source_synced_at,

        jr._loaded_at

    from job_reports_ranked as jr
    left join well_360 as w360
        on jr.well_id = w360.wellview_id
    left join daily_costs_agg as dc
        on jr.report_id = dc.job_report_id
    left join time_log_by_report_id as tl_id
        on jr.report_id = tl_id.job_report_id
    left join time_log_by_report_number as tl_rn
        on
            jr.job_id = tl_rn.job_id
            and jr.report_number = tl_rn.report_number
    left join time_log_by_report_date as tl_dt
        on
            jr.job_id = tl_dt.job_id
            and jr.report_date = tl_dt.event_date
            and jr.report_date_rank = 1
    left join npt_by_report_number as npt_rn
        on
            jr.job_id = npt_rn.job_id
            and jr.report_number = npt_rn.report_number
    left join npt_by_report_date as npt_dt
        on
            jr.job_id = npt_dt.job_id
            and jr.report_date = npt_dt.event_date
            and jr.report_date_rank = 1
    left join safety_by_report_number as safety_rn
        on
            jr.job_id = safety_rn.job_id
            and jr.report_number = safety_rn.report_number
    left join safety_by_report_date as safety_dt
        on
            jr.job_id = safety_dt.job_id
            and jr.report_date = safety_dt.event_date
            and jr.report_date_rank = 1
)

select * from enriched
