{{
    config(
        materialized='incremental',
        unique_key='completiondaily_sk',
        tags=['griffin', 'marts', 'crescent']
    )
}}

/*
    Daily production fact for Griffin namespace.
    Completion-level daily volumes with allocated and measured values.
*/

with

daily as (

    select * from {{ ref('int_griffin__daily_production') }}
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
        d.completiondaily_sk,
        c.completion_key,
        d.merrick_id,
        d.record_date,

        -- completion context (denormalized for reporting)
        d.well_name,
        d.completion_name,
        d.wellpluscompletion_name,
        d.route_name,
        d.property_number,

        -- status
        d.producingstatus,
        d.producingmethod,
        d.producing_status_description,
        d.producing_method_description,

        -- allocated volumes (primary)
        d.alloc_est_oil_vol as alloc_oil_bbls,
        d.alloc_est_gas_vol_mcf as alloc_gas_mcf,
        d.alloc_est_gas_vol_mmbtu as alloc_gas_mmbtu,
        d.alloc_est_water_vol as alloc_water_bbls,
        d.alloc_est_ngl_vol as alloc_ngl_bbls,
        d.alloc_est_other_vol as alloc_other_bbls,
        d.alloc_est_co2_vol as alloc_co2_bbls,
        d.alloc_boe,

        -- entered/measured volumes
        d.oilproduction as meas_oil_bbls,
        d.waterproduction as meas_water_bbls,
        d.enteredoilvol,
        d.enteredgas_vol_mcf,
        d.enteredwatervol,
        d.enterednglvol,
        d.enteredothervol,

        -- gas disposition
        d.gas_lease_use_mcf,
        d.gas_flare_mcf as flare_gas_mcf,
        d.gas_vent_mcf as vent_gas_mcf,
        d.gas_lift_vol as lift_gas_vol,
        d.gas_sales_mcf,
        d.gas_sales_mmbtu,

        -- plant gas
        d.alloc_est_plant_gas_mcf as plant_gas_mcf,
        d.alloc_est_plant_gas_mmbtu as plant_gas_mmbtu,

        -- injection
        d.alloc_est_inj_gas_vol_mcf as inj_gas_mcf,
        d.alloc_est_inj_water_vol as inj_water_bbls,
        d.alloc_est_inj_oil_vol as inj_oil_bbls,
        d.alloc_est_inj_co2_vol as inj_co2_bbls,
        d.gasinjection,
        d.waterinjection,
        d.oilinjection,

        -- inventory
        d.beginningoil,
        d.endingoil,
        d.beginningwater,
        d.endingwater,

        -- operations
        d.hoursflowed as hours_on,
        d.hoursidle as hours_down,
        d.hoursrecovery,
        d.hoursinjected,
        d.dailydowntime,
        d.total_hours,
        d.uptime_pct,
        d.chokesize as choke_size,
        d.choke,
        d.pumpdepth,

        -- pressures
        d.casingpressure as casing_pressure,
        d.tubingpressure as tubing_pressure,
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

        -- gas properties
        d.alloc_est_btu_factor as btu_factor,
        d.alloc_est_gravity as gravity,
        d.enteredbtufactor,
        d.enteredgravity,
        d.gasoilratio as gor,
        d.alloc_est_wet_dry_flag,

        -- allocation
        d.allocationfactor,
        d.allocationfactor_type,
        d.disposition_code,
        d.product_code,
        d.product_type,

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

)

select * from final
