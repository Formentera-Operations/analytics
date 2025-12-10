{{
    config(
        materialized='view',
        tags=['griffin', 'intermediate', 'crescent']
    )
}}

/*
    Intermediate model for downtime events.
    Joins downtime reasons with downtime codes for full context.
*/

with

downtime_reasons as (

    select * from {{ ref('stg_procount__downtime_reasons') }}

),

downtime_codes as (

    select
        downtime_code,
        downtimedescription,
        downtimedescriptionlevel1,
        downtimedescriptionlevel2,
        downtimedescriptionlevel3,
        downtimedescriptionlevel4,
        active_flag as code_active_flag,
        updown_flag,
        producingstatus,
        producingmethod,
        defaultproducingstatus
    from {{ ref('stg_procount__downtime_codes') }}

),

completions as (

    select
        merrick_id,
        well_name,
        completion_name,
        wellpluscompletion_name,
        route_id,
        route_name,
        property_number
    from {{ ref('int_griffin__completions_enriched') }}

),

enriched as (

    select
        -- grain
        dr.downtimereason_sk,
        dr.object_merrick_id as merrick_id,
        dr.object_type,
        dr.originaldateentered as downtime_date,
        dr.originaltimeentered as downtime_time,

        -- completion context (when object_type matches completion)
        c.well_name,
        c.completion_name,
        c.wellpluscompletion_name,
        c.route_id,
        c.route_name,
        c.property_number,

        -- downtime details
        dr.downtime_code,
        dc.downtimedescription,
        dc.downtimedescriptionlevel1,
        dc.downtimedescriptionlevel2,
        dc.downtimedescriptionlevel3,
        dc.downtimedescriptionlevel4,
        dc.updown_flag,
        dr.downtime_hours,
        dr.reason as downtime_reason,
        dr.comments,

        -- date range
        dr.start_date,
        dr.starttime,
        dr.end_date,
        dr.endtime,
        dr.startproduction_date,
        dr.endproduction_date,

        -- calculated duration
        case
            when dr.start_date is not null and dr.end_date is not null
            then datediff('hour', 
                to_timestamp(to_varchar(dr.start_date::date, 'YYYY-MM-DD') || ' ' || coalesce(dr.starttime, '00:00:00')),
                to_timestamp(to_varchar(dr.end_date::date, 'YYYY-MM-DD') || ' ' || coalesce(dr.endtime, '00:00:00'))
            )
            else dr.downtime_hours
        end as calculated_hours,

        -- lost production
        dr.lostproduction,

        -- boe lost (assuming lostproduction is oil equivalent)
        coalesce(dr.lostproduction, 0) as lost_boe,

        -- costs
        dr.repaircosts,

        -- last day impact
        dr.lastdayhoursdown,

        -- flags
        dr.transmit_flag,
        dr.messagesend_flag,
        dr.delete_flag,
        dr.calcdowntime_flag,
        dr.dateentry_flag,

        -- status changes
        dc.producingstatus as code_producingstatus,
        dc.producingmethod as code_producingmethod,
        dc.defaultproducingstatus,

        -- metadata
        dr._fivetran_synced,
        dr._loaded_at

    from downtime_reasons dr
    left join downtime_codes dc
        on dr.downtime_code = dc.downtime_code
    left join completions c
        on dr.object_merrick_id = c.merrick_id

),

final as (

    select * from enriched

)

select * from final