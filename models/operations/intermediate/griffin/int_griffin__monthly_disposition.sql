{{
    config(
        materialized='view',
        tags=['griffin', 'intermediate', 'crescent']
    )
}}

/*
    Intermediate model for monthly disposition details at completion level.
    Aggregated disposition volumes for detailed sales/fuel/flare analysis.
*/

with

monthly_disp as (

    select * from {{ ref('stg_procount__completionmonthlydisp') }}

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
        d.completionmonthlydisp_sk,
        d.merrick_id,
        d.record_date,
        date_trunc('month', d.record_date) as production_month,
        d.run_ticket_number,
        d.source_id,
        d.source_type,
        d.gathering_system_id,

        -- completion context
        c.well_name,
        c.completion_name,
        c.wellpluscompletion_name,
        c.route_id,
        c.route_name,
        c.property_number,

        -- disposition codes
        d.disposition_code,
        d.product_code,
        d.product_type,

        -- volumes (allocact = allocated actual)
        d.allocactoilvol as alloc_oil_vol,
        d.allocactgas_vol_mcf as alloc_gas_vol_mcf,
        d.allocactgas_vol_mmbtu as alloc_gas_vol_mmbtu,
        d.allocactwatervol as alloc_water_vol,
        d.allocactnglvol as alloc_ngl_vol,
        d.allocactothervol as alloc_other_vol,
        d.allocactco2vol as alloc_co2_vol,
        d.allocactmass as alloc_mass,

        -- injection volumes
        d.allocactinjoilvol as alloc_inj_oil_vol,
        d.allocactinjgas_vol_mcf as alloc_inj_gas_vol_mcf,
        d.allocactinjgas_vol_mmbtu as alloc_inj_gas_vol_mmbtu,
        d.allocactinjwatervol as alloc_inj_water_vol,
        d.allocactinjco2vol as alloc_inj_co2_vol,
        d.allocactinjothervol as alloc_inj_other_vol,

        -- plant gas
        d.allocactplantgasmcf as alloc_plant_gas_mcf,
        d.allocactplantgasmmbtu as alloc_plant_gas_mmbtu,

        -- quality
        d.allocactgravity as alloc_gravity,
        d.allocactbtufactor as alloc_btu_factor,
        d.allocactpressurebase as alloc_pressure_base,
        d.allocacttemperature as alloc_temperature,

        -- boe calculation
        coalesce(d.allocactoilvol, 0) 
            + coalesce(d.allocactnglvol, 0) 
            + (coalesce(d.allocactgas_vol_mcf, 0) / 6.0) as alloc_boe,

        -- flags
        d.run_ticket_flag,
        d.overwrite_flag,
        d.allocactwetdry_flag,

        -- dates
        d.run_ticket_date,
        d.allocation_date_stamp,

        -- metadata
        d._fivetran_synced,
        d._loaded_at

    from monthly_disp d
    left join completions c
        on d.merrick_id = c.merrick_id

),

final as (

    select * from enriched

)

select * from final
