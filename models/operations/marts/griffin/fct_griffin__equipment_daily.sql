{{
    config(
        materialized='incremental',
        unique_key='equipmentdaily_sk',
        tags=['griffin', 'marts', 'crescent']
    )
}}

/*
    Equipment daily fact for Griffin namespace.
    Daily operational data for pumps, compressors, and other equipment.
*/

with

daily as (

    select * from {{ ref('stg_procount__equipment_daily') }}
    {% if is_incremental() %}
    where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})
    {% endif %}

),

equipment as (

    select
        equipment_key,
        equipment_merrick_id,
        completion_merrick_id,
        equipmentdescription as equipment_name,
        equipment_type,
        route_id,
        property_number
    from {{ ref('dim_griffin__equipment') }}

),

completions as (

    select
        completion_key,
        merrick_id,
        well_name,
        route_name
    from {{ ref('dim_griffin__completions') }}

),

final as (

    select
        -- surrogate key
        d.equipmentdaily_sk,
        
        -- dimension keys
        e.equipment_key,
        c.completion_key,
        
        -- natural keys
        d.merrick_id as equipment_merrick_id,
        e.completion_merrick_id,
        d.record_date,

        -- equipment context
        e.equipment_name,
        e.equipment_type,
        c.well_name,
        c.route_name,
        e.property_number,

        -- downtime
        d.downtime_code,
        d.downtime_hours,
        d.stilldown_flag,

        -- operations
        d.hourson as hours_on,
        d.hoursopen as hours_open,
        d.hoursclose as hours_close,
        d.strokesperminute as strokes_per_minute,
        d.enginerpm as engine_rpm,

        -- actual volumes
        d.actoilvol as act_oil_vol,
        d.actgas_vol_mcf,
        d.actgas_vol_mmbtu,
        d.actwatervol as act_water_vol,
        d.actothervol as act_other_vol,

        -- converted volumes
        d.convoilvol as conv_oil_vol,
        d.convgas_vol_mcf,
        d.convgas_vol_mmbtu,
        d.convwatervol as conv_water_vol,
        d.convothervol as conv_other_vol,

        -- rates
        d.rateoil as rate_oil,
        d.rategas as rate_gas,
        d.ratewater as rate_water,

        -- pressures
        d.suctionpressure as suction_pressure,
        d.dischargepressure as discharge_pressure,
        d.intakepressure as intake_pressure,
        d.pumpingpressure as pumping_pressure,
        d.stage1pressure,
        d.stage2pressure,
        d.stage3pressure,
        d.stage4pressure,
        d.engineoilpressure as engine_oil_pressure,
        d.compressoroilpressure as compressor_oil_pressure,
        d.oilpressure as oil_pressure,
        d.actpressurebase as act_pressure_base,
        d.convpressurebase as conv_pressure_base,

        -- temperatures
        d.acttemperature as act_temperature,
        d.convtemperature as conv_temperature,
        d.suctiontemp as suction_temp,
        d.dischargetemp as discharge_temp,
        d.stage1temp,
        d.stage2temp,
        d.stage3temp,
        d.stage4temp,
        d.engineoiltemp as engine_oil_temp,
        d.enginewatertemp as engine_water_temp,
        d.compressoroiltemp as compressor_oil_temp,
        d.compressorwatertemp as compressor_water_temp,
        d.inletoiltemp as inlet_oil_temp,
        d.outletoiltemp as outlet_oil_temp,
        d.exchangetemp as exchange_temp,
        d.exhaust1temp as exhaust1_temp,
        d.exhaust2temp as exhaust2_temp,

        -- quality/allocation factors
        d.actgravity as act_gravity,
        d.convgravity as conv_gravity,
        d.actbtufactor as act_btu_factor,
        d.convbtufactor as conv_btu_factor,
        d.actheatfactor as act_heat_factor,
        d.convheatfactor as conv_heat_factor,
        d.leaseusecoefficient as lease_use_coefficient,
        d.leaseusecoefficient_type as lease_use_coefficient_type,

        -- boost
        d.suctionorboost1 as suction_or_boost1,
        d.suctionorboost2 as suction_or_boost2,

        -- allocation
        d.allocationmethod as allocation_method,
        d.allocationorder as allocation_order,
        d.allocationbycomponent as allocation_by_component,
        d.disposition_code,
        d.product_code,
        d.product_type,
        d.datasource_code,

        -- other
        d.lubricationoilused as lubrication_oil_used,
        d.taxable,
        d.error_number,

        -- flags
        d.backgroundtask_flag,
        d.transmit_flag,
        d.calculationstatus_flag,
        d.volumeautopopulate_flag,
        d.hoursonautopopulate_flag,
        d.actwetdry_flag,
        d.convwetdry_flag,
        d.ratecomputation_flag,

        -- metadata
        d._fivetran_synced,
        d._loaded_at

    from daily d
    left join equipment e
        on d.merrick_id = e.equipment_merrick_id
    left join completions c
        on e.completion_merrick_id = c.merrick_id

)

select * from final
