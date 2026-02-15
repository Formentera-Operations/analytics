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
    select
        wellview_id,
        eid
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
        not coalesce(ip.exclude_from_problem_time_calculations, false) as is_countable_npt,

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
        is_countable_npt,
        _loaded_at
    from enriched
)

select * from final
