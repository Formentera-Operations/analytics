{{
    config(
        materialized='view',
        tags=['griffin', 'intermediate', 'crescent']
    )
}}

/*
    Intermediate model for daily production at completion level.
    Joins daily volumes with completion context and calculates key metrics.
*/

with

daily as (

    select * from {{ ref('stg_procount__completiondaily') }}

),

completions as (

    select * from {{ ref('int_griffin__completions_enriched') }}

),

joined as (

    select
        -- grain
        d.completiondaily_sk,
        d.merrick_id,
        d.record_date,

        -- completion context
        c.well_name,
        c.completion_name,
        c.wellpluscompletion_name,
        c.uwi,
        c.apiwell_number,
        c.route_id,
        c.route_name,
        c.gathering_system_id,
        c.gathering_system_name,
        c.lease_id,
        c.area_id,
        c.division_id,
        c.property_number,

        -- status on this day
        d.producingstatus,
        d.producingmethod,
        c.producing_status_description,
        c.producing_method_description,

        -- allocated volumes (primary reporting)
        d.alloc_est_oil_vol,
        d.alloc_est_gas_vol_mcf,
        d.alloc_est_gas_vol_mmbtu,
        d.alloc_est_water_vol,
        d.alloc_est_ngl_vol,
        d.alloc_est_other_vol,
        d.alloc_est_co2_vol,

        -- entered/measured volumes
        d.oilproduction,
        d.waterproduction,
        d.enteredoilvol,
        d.enteredgas_vol_mcf,
        d.enteredgas_vol_mmbtu,
        d.enteredwatervol,
        d.enterednglvol,
        d.enteredothervol,

        -- injection volumes
        d.alloc_est_inj_oil_vol,
        d.alloc_est_inj_gas_vol_mcf,
        d.alloc_est_inj_water_vol,
        d.alloc_est_inj_co2_vol,
        d.alloc_est_inj_other_vol,
        d.gasinjection,
        d.waterinjection,
        d.oilinjection,

        -- plant gas
        d.alloc_est_plant_gas_mcf,
        d.alloc_est_plant_gas_mmbtu,

        -- gas disposition detail
        d.enteredgasvolmcfleaseuse as gas_lease_use_mcf,
        d.enteredgasflaremcf as gas_flare_mcf,
        d.enteredgasventedmcf as gas_vent_mcf,
        d.allocestgasliftvol as gas_lift_vol,
        d.enteredgassales_vol_mcf as gas_sales_mcf,
        d.enteredgassales_vol_mmbtu as gas_sales_mmbtu,

        -- inventory
        d.beginningoil,
        d.endingoil,
        d.beginningwater,
        d.endingwater,

        -- pressures
        d.casingpressure,
        d.tubingpressure,
        d.surfacecasingpressure,
        d.intercasingpressure,
        d.linercasingpressure,
        d.shutintubingpressure,
        d.secondtubingpressure,
        d.injectionpressure,
        d.alloc_est_pressure_base,

        -- temperatures
        d.alloc_est_temperature,
        d.enteredtemperature,
        d.bhtemperature,
        d.tubingtemperature,

        -- choke/operations
        d.chokesize,
        d.choke,
        d.hoursflowed,
        d.hoursidle,
        d.hoursrecovery,
        d.hoursinjected,
        d.dailydowntime,
        d.pumpdepth,

        -- gas analysis/quality
        d.alloc_est_btu_factor,
        d.alloc_est_gravity,
        d.enteredbtufactor,
        d.enteredgravity,
        d.gasoilratio,
        d.alloc_est_wet_dry_flag,

        -- allocation factors
        d.allocationfactor,
        d.allocationfactor_type,

        -- disposition/product codes
        d.disposition_code,
        d.product_code,
        d.product_type,

        -- calculated fields
        coalesce(d.hoursflowed, 0) + coalesce(d.hoursidle, 0) as total_hours,
        case 
            when coalesce(d.hoursflowed, 0) + coalesce(d.hoursidle, 0) > 0 
            then round(coalesce(d.hoursflowed, 0) / (coalesce(d.hoursflowed, 0) + coalesce(d.hoursidle, 0)) * 100, 2)
            else null 
        end as uptime_pct,

        -- boe calculations (6:1 gas conversion)
        coalesce(d.alloc_est_oil_vol, 0) 
            + coalesce(d.alloc_est_ngl_vol, 0) 
            + (coalesce(d.alloc_est_gas_vol_mcf, 0) / 6.0) as alloc_boe,

        -- flags
        d.stilldown_flag,
        d.backgroundtask_flag,
        d.transmit_flag,

        -- metadata
        d._fivetran_synced,
        d._loaded_at

    from daily d
    left join completions c
        on d.merrick_id = c.merrick_id

),

final as (

    select * from joined

)

select * from final
