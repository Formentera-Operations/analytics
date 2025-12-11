{{
    config(
        materialized='incremental',
        unique_key='run_ticket_sk',
        tags=['griffin', 'marts', 'crescent']
    )
}}

/*
    Run tickets fact for Griffin namespace.
    Unified view of tank and meter hauling/sales tickets.
*/

with

tickets as (

    select * from {{ ref('int_griffin__run_tickets_unified') }}
    {% if is_incremental() %}
    where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})
    {% endif %}

),

tanks as (

    select
        tank_key,
        tank_merrick_id,
        completion_merrick_id,
        well_name,
        tank_name,
        route_name,
        property_number
    from {{ ref('dim_griffin__tanks') }}

),

meters as (

    select
        meter_key,
        meter_merrick_id,
        completion_merrick_id,
        well_name,
        meter_name,
        route_name,
        property_number
    from {{ ref('dim_griffin__meters') }}

),

completions as (

    select
        completion_key,
        merrick_id
    from {{ ref('dim_griffin__completions') }}

),

enriched as (

    select
        t.run_ticket_sk,
        t.merrick_id as equipment_merrick_id,
        t.record_date,
        t.run_ticket_number,
        t.equipment_type,
        
        -- equipment context based on type
        case 
            when t.equipment_type = 'TANK' then tk.tank_key
            else null
        end as tank_key,
        case 
            when t.equipment_type = 'METER' then m.meter_key
            else null
        end as meter_key,
        
        -- completion key (via equipment parent)
        coalesce(
            c_tank.completion_key,
            c_meter.completion_key
        ) as completion_key,
        
        -- context
        coalesce(tk.well_name, m.well_name) as well_name,
        coalesce(tk.tank_name, m.meter_name) as equipment_name,
        coalesce(tk.route_name, m.route_name) as route_name,
        coalesce(tk.property_number, m.property_number) as property_number,

        -- ticket details
        t.ticket_date,
        t.ticket_time,
        t.runopen_date,
        t.runopentime,
        t.runclose_date,
        t.runclosetime,

        -- purchaser/hauler
        t.purchaser_id,
        t.purchaserloc,
        t.hauler_id,
        t.haulerloc,
        t.waterpurchaser,
        t.waterhauler,

        -- volumes
        t.gross_volume,
        t.net_volume,
        t.haulerreportedbarrels,
        t.haulerreportednetbarrels,
        t.allocatedoilbarrels,
        t.allocatedwaterbarrels,
        t.allocatedngl,
        t.netwaterbarrels,

        -- quality
        t.bs_and_w,
        t.gravity,
        t.convertedgravity,
        t.allocatedgravity,
        t.temperature,
        t.opentemperature,
        t.closetemperature,
        t.watercut,
        t.emulsionpercent,

        -- correction factors
        t.lactmeterfactor,
        t.lactcompressibilityfactor,
        t.lactdensitycorrection,

        -- classification
        t.disposition_code,
        t.product_code,
        t.product_type,
        t.fluiddisposition,

        -- gauge readings (tanks)
        t.openfeet,
        t.openinch,
        t.openquarter,
        t.topopentotalinches,
        t.closefeet,
        t.closeinch,
        t.closequarter,
        t.topclosetotalinches,

        -- odometer
        t.openodometer,
        t.closeodometer,

        -- seals (tanks)
        t.sealon,
        t.sealoff,
        t.sealdistribution_code,

        -- destination
        t.destination,
        t.destinationobject_id,
        t.destinationobject_type,
        t.originationobject_id,
        t.originationobject_type,

        -- statements
        t.purchaserstatement_id,
        t.haulerstatement_id,
        t.haulerstatement_date,

        -- flags
        t.actualestimated_flag,
        t.delete_flag,
        t.transmit_flag,
        t.backgroundtask_flag,
        t.usehaulerbarrels_flag,
        t.runordisposition_flag,
        t.calculationstatus_flag,
        t.oilwaterrun_flag,
        t.allocatedticket_flag,
        t.units_flag,

        -- metadata
        t._fivetran_synced,
        t._loaded_at

    from tickets t
    left join tanks tk
        on t.equipment_type = 'TANK'
        and t.merrick_id = tk.tank_merrick_id
    left join meters m
        on t.equipment_type = 'METER'
        and t.merrick_id = m.meter_merrick_id
    left join completions c_tank
        on tk.completion_merrick_id = c_tank.merrick_id
    left join completions c_meter
        on m.completion_merrick_id = c_meter.merrick_id

),

final as (

    select * from enriched

)

select * from final
