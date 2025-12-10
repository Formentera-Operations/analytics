{{
    config(
        materialized='incremental',
        unique_key='downtimereason_sk',
        tags=['griffin', 'marts', 'crescent']
    )
}}

/*
    Downtime fact for Griffin namespace.
    Completion downtime events with codes and lost production.
*/

with

downtime as (

    select * from {{ ref('int_griffin__downtime_events') }}
    {% if is_incremental() %}
    where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})
    {% endif %}

),

completions as (

    select
        completion_key,
        merrick_id
    from {{ ref('dim_griffin__completions') }}

),

final as (

    select
        -- keys
        d.downtimereason_sk,
        c.completion_key,
        d.merrick_id,
        d.object_type,

        -- timing
        d.downtime_date,
        d.downtime_time,
        d.start_date,
        d.starttime,
        d.end_date,
        d.endtime,
        d.startproduction_date,
        d.endproduction_date,

        -- completion context
        d.well_name,
        d.completion_name,
        d.wellpluscompletion_name,
        d.route_name,
        d.property_number,

        -- downtime classification
        d.downtime_code,
        d.downtimedescription,
        d.downtimedescriptionlevel1,
        d.downtimedescriptionlevel2,
        d.downtimedescriptionlevel3,
        d.downtimedescriptionlevel4,
        d.updown_flag,
        d.downtime_reason,
        d.comments,

        -- duration
        d.downtime_hours,
        d.calculated_hours,
        d.lastdayhoursdown,

        -- lost production
        d.lostproduction as lost_production,
        d.lost_boe,

        -- costs
        d.repaircosts as repair_costs,

        -- status changes
        d.code_producingstatus,
        d.code_producingmethod,
        d.defaultproducingstatus,

        -- flags
        d.transmit_flag,
        d.messagesend_flag,
        d.delete_flag,
        d.calcdowntime_flag,
        d.dateentry_flag,

        -- metadata
        d._fivetran_synced,
        d._loaded_at

    from downtime d
    left join completions c
        on d.merrick_id = c.merrick_id

)

select * from final
