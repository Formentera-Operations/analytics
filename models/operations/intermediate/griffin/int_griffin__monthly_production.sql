{{
    config(
        materialized='view',
        tags=['griffin', 'intermediate', 'crescent']
    )
}}

/*
    Intermediate model for monthly production at completion level.
    Joins monthly volumes with completion context.
*/

with

monthly as (

    select * from {{ ref('stg_procount__completionmonthly') }}

),

completions as (

    select * from {{ ref('int_griffin__completions_enriched') }}

),

joined as (

    select
        -- grain
        m.completionmonthly_sk,
        m.merrick_id,
        m.record_date,
        date_trunc('month', m.record_date) as production_month,
        m.start_date,
        m.end_date,

        -- completion context
        c.well_name,
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

        -- monthly table also has some completion attrs
        m.well_name as monthly_well_name,
        m.completion_name,
        m.completion_number,
        m.well_number,
        m.property_number as monthly_property_number,

        -- status
        m.producingstatus,
        m.producingmethod,
        c.producing_status_description,
        c.producing_method_description,

        -- allocated actual volumes (allocact = allocated actual)
        m.allocactoilvol as alloc_oil_vol,
        m.allocactgas_vol_mcf as alloc_gas_vol_mcf,
        m.allocactgas_vol_mmbtu as alloc_gas_vol_mmbtu,
        m.allocactwatervol as alloc_water_vol,
        m.allocactnglvol as alloc_ngl_vol,
        m.allocactothervol as alloc_other_vol,
        m.allocactco2vol as alloc_co2_vol,

        -- net volumes
        m.allocactnetoilvol as alloc_net_oil_vol,
        m.allocactnetgasvol as alloc_net_gas_vol,
        m.allocactnetwatervol as alloc_net_water_vol,

        -- sales gas
        m.allocactsalesgas_vol_mcf as alloc_sales_gas_mcf,

        -- entered/measured volumes
        m.oilproduction,
        m.waterproduction,
        m.enteredoilvol,
        m.enteredgas_vol_mcf,
        m.enteredgas_vol_mmbtu,
        m.enteredwatervol,
        m.enterednglvol,
        m.enteredothervol,

        -- estimated volumes
        m.estoilvol,
        m.estgas_vol_mcf,
        m.estgas_vol_mmbtu,
        m.estwatervol,
        m.estnglvol,
        m.estothervol,

        -- injection volumes
        m.allocactinjoilvol,
        m.allocactinjgas_vol_mcf,
        m.allocactinjgas_vol_mmbtu,
        m.allocactinjwatervol,
        m.allocactinjco2vol,
        m.allocactinjothervol,

        -- plant gas
        m.allocactplantgasmmbtu as alloc_plant_gas_mmbtu,
        m.allocactplantgasmcf as alloc_plant_gas_mcf,

        -- gas lift
        m.allocactgasliftmcf as alloc_gas_lift_mcf,

        -- lease use
        m.enteredgasvolmcfleaseuse as gas_lease_use_mcf,
        m.leaseuse,
        m.leaseuseproduction,
        m.leaseusemarket,
        m.leaseusecompressor,

        -- flare/vent
        m.enteredgasflaremcf as gas_flare_mcf,
        m.enteredgasventedmcf as gas_vent_mcf,

        -- sales volumes
        m.enteredgassales_vol_mcf as entered_gas_sales_mcf,
        m.enteredgassales_vol_mmbtu as entered_gas_sales_mmbtu,

        -- cumulative volumes
        m.cumulativeoil,
        m.cumulativegas,
        m.cumulativewater,
        m.cumulativeinjgas,
        m.cumulativeinjoil,
        m.cumulativeinjwater,

        -- quality
        m.allocactgravity as alloc_gravity,
        m.allocactbtufactor as alloc_btu_factor,
        m.allocactpressurebase as alloc_pressure_base,
        m.allocacttemperature as alloc_temperature,
        m.estgravity,
        m.estbtufactor,
        m.gasoilratio,

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
        m.dayson as producing_days,
        m.daysidle as days_idle,
        m.daysinjected,
        m.totalhoursflowed,
        m.totalhoursidle,
        m.totalhoursrecovery,
        m.totalhoursinjected,
        m.totaldowntime,

        -- calculated fields
        case 
            when coalesce(m.dayson, 0) + coalesce(m.daysidle, 0) > 0 
            then round(coalesce(m.dayson, 0) / (coalesce(m.dayson, 0) + coalesce(m.daysidle, 0)) * 100, 2)
            else null 
        end as uptime_pct,

        -- boe calculations (6:1 gas conversion)
        coalesce(m.allocactoilvol, 0) 
            + coalesce(m.allocactnglvol, 0) 
            + (coalesce(m.allocactgas_vol_mcf, 0) / 6.0) as alloc_boe,

        -- daily averages
        case when m.dayson > 0 then m.allocactoilvol / m.dayson else null end as avg_daily_oil,
        case when m.dayson > 0 then m.allocactgas_vol_mcf / m.dayson else null end as avg_daily_gas_mcf,
        case when m.dayson > 0 then m.allocactwatervol / m.dayson else null end as avg_daily_water,

        -- targets
        m.oiltargetprod,
        m.gastargetprod,

        -- disposition/product codes  
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

),

final as (

    select * from joined

)

select * from final
