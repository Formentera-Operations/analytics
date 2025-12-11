{{
    config(
        materialized='incremental',
        unique_key='completionmonthly_sk',
        tags=['griffin', 'marts', 'crescent']
    )
}}

/*
    Monthly production fact for Griffin namespace.
    Completion-level monthly volumes with allocated actual values.
*/

with

monthly as (

    select * from {{ ref('int_griffin__monthly_production') }}
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
        m.completionmonthly_sk,
        c.completion_key,
        m.merrick_id,
        m.record_date,
        m.production_month,
        m.start_date,
        m.end_date,

        -- completion context (denormalized for reporting)
        m.well_name,
        m.completion_name,
        m.wellpluscompletion_name,
        m.route_name,
        m.property_number,
        m.monthly_property_number,

        -- status
        m.producingstatus,
        m.producingmethod,
        m.producing_status_description,
        m.producing_method_description,

        -- allocated actual volumes (primary - allocact prefix)
        m.alloc_oil_vol as alloc_oil_bbls,
        m.alloc_gas_vol_mcf as alloc_gas_mcf,
        m.alloc_gas_vol_mmbtu as alloc_gas_mmbtu,
        m.alloc_water_vol as alloc_water_bbls,
        m.alloc_ngl_vol as alloc_ngl_bbls,
        m.alloc_other_vol as alloc_other_bbls,
        m.alloc_co2_vol as alloc_co2_bbls,
        m.alloc_boe,

        -- net volumes
        m.alloc_net_oil_vol as alloc_net_oil_bbls,
        m.alloc_net_gas_vol as alloc_net_gas_mcf,
        m.alloc_net_water_vol as alloc_net_water_bbls,

        -- sales gas
        m.alloc_sales_gas_mcf,

        -- entered/measured volumes
        m.oilproduction as meas_oil_bbls,
        m.waterproduction as meas_water_bbls,
        m.enteredoilvol,
        m.enteredgas_vol_mcf,
        m.enteredwatervol,
        m.enterednglvol,
        m.enteredothervol,

        -- estimated volumes
        m.estoilvol,
        m.estgas_vol_mcf,
        m.estwatervol,
        m.estnglvol,

        -- gas disposition
        m.gas_lease_use_mcf,
        m.gas_flare_mcf as flare_gas_mcf,
        m.gas_vent_mcf as vent_gas_mcf,
        m.alloc_gas_lift_mcf as lift_gas_mcf,
        m.entered_gas_sales_mcf,
        m.entered_gas_sales_mmbtu,

        -- lease use breakdown
        m.leaseuse,
        m.leaseuseproduction,
        m.leaseusemarket,
        m.leaseusecompressor,

        -- plant gas
        m.alloc_plant_gas_mcf as plant_gas_mcf,
        m.alloc_plant_gas_mmbtu as plant_gas_mmbtu,

        -- injection
        m.allocactinjgas_vol_mcf as inj_gas_mcf,
        m.allocactinjwatervol as inj_water_bbls,
        m.allocactinjoilvol as inj_oil_bbls,
        m.allocactinjco2vol as inj_co2_bbls,
        m.allocactinjothervol as inj_other_bbls,

        -- cumulative production
        m.cumulativeoil,
        m.cumulativegas,
        m.cumulativewater,
        m.cumulativeinjgas,
        m.cumulativeinjoil,
        m.cumulativeinjwater,

        -- run tickets
        m.oilruntickets,
        m.waterruntickets,

        -- reserves
        m.oilreserve,
        m.gasreserve,
        m.waterreserve,
        m.reservesoil,
        m.reservesgas,
        m.reserveswater,

        -- operations
        m.producing_days,
        m.days_idle,
        m.daysinjected,
        m.totalhoursflowed,
        m.totalhoursidle,
        m.totalhoursrecovery,
        m.totalhoursinjected,
        m.totaldowntime,
        m.uptime_pct,

        -- daily averages
        m.avg_daily_oil,
        m.avg_daily_gas_mcf,
        m.avg_daily_water,

        -- targets
        m.oiltargetprod,
        m.gastargetprod,

        -- quality
        m.alloc_gravity as gravity,
        m.alloc_btu_factor as btu_factor,
        m.alloc_pressure_base,
        m.alloc_temperature,
        m.estgravity,
        m.estbtufactor,
        m.gasoilratio as gor,

        -- disposition/product
        m.disposition_code,
        m.product_code,
        m.product_type,

        -- flags
        m.summation_flag,
        m.backgroundtask_flag,
        m.transmit_flag,

        -- metadata
        m._fivetran_synced,
        m._loaded_at

    from monthly m
    left join completions c
        on m.merrick_id = c.merrick_id

)

select * from final
