{{
    config(
        materialized='table',
        tags=['drilling', 'mart', 'fact']
    )
}}

with safety_checks as (
    select
        safety_check_sk as safety_event_sk,
        record_id,
        well_id,
        parent_record_id as job_id,
        'check' as event_type,

        -- temporal
        check_datetime as event_datetime,
        check_datetime::date as event_date,
        report_number,
        tour,

        -- classification
        check_type as type_primary,
        null::varchar as type_secondary,
        null::varchar as category,
        check_result as result_or_severity,
        null::varchar as potential_severity,
        null::varchar as cause,

        -- people
        inspector as responsible_person,
        null::varchar as witness,
        null::varchar as rig_crew_name,

        -- measures
        null::float as lost_time_hours,
        null::float as estimated_cost,

        -- flags
        false as is_reportable,
        false as is_lost_time_incident,

        -- descriptive
        description,
        comment,

        -- dbt metadata
        _loaded_at

    from {{ ref('stg_wellview__safety_checks') }}
),

safety_incidents as (
    select
        safety_incident_sk as safety_event_sk,
        record_id,
        well_id,
        parent_record_id as job_id,
        'incident' as event_type,

        -- temporal
        incident_datetime as event_datetime,
        incident_datetime::date as event_date,
        report_number,
        tour,

        -- classification
        incident_type as type_primary,
        incident_subtype as type_secondary,
        category,
        severity as result_or_severity,
        potential_severity,
        cause,

        -- people
        reported_by as responsible_person,
        witness,
        rig_crew_name,

        -- measures
        lost_time_hours,
        estimated_cost,

        -- flags
        is_reportable,
        is_lost_time_incident,

        -- descriptive
        description,
        comment,

        -- dbt metadata
        _loaded_at

    from {{ ref('stg_wellview__safety_incidents') }}
),

unioned as (
    select * from safety_checks
    union all
    select * from safety_incidents
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
        u.safety_event_sk,

        -- dimensional FKs
        {{ dbt_utils.generate_surrogate_key(['u.job_id']) }} as job_sk,
        w360.eid,

        -- natural keys
        u.record_id,
        u.well_id,
        u.job_id,

        -- event identity
        u.event_type,
        u.event_datetime,
        u.event_date,
        u.report_number,
        u.tour,

        -- classification
        u.type_primary,
        u.type_secondary,
        u.category,
        u.result_or_severity,
        u.potential_severity,
        u.cause,

        -- people
        u.responsible_person,
        u.witness,
        u.rig_crew_name,

        -- measures
        u.lost_time_hours,
        u.estimated_cost,

        -- flags
        u.is_reportable,
        u.is_lost_time_incident,

        -- descriptive
        u.description,
        u.comment,

        -- dbt metadata
        u._loaded_at

    from unioned as u
    left join well_360 as w360
        on u.well_id = w360.wellview_id
),

final as (
    select
        safety_event_sk,
        job_sk,
        eid,
        record_id,
        well_id,
        job_id,
        event_type,
        event_datetime,
        event_date,
        report_number,
        tour,
        type_primary,
        type_secondary,
        category,
        result_or_severity,
        potential_severity,
        cause,
        responsible_person,
        witness,
        rig_crew_name,
        lost_time_hours,
        estimated_cost,
        is_reportable,
        is_lost_time_incident,
        description,
        comment,
        _loaded_at
    from enriched
)

select * from final
