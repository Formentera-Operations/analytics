{{
    config(
        materialized='view',
        tags=['griffin', 'intermediate', 'crescent']
    )
}}

/*
    Intermediate model for daily disposition details at completion level.
    Unpivots disposition volumes for detailed sales/fuel/flare analysis.
*/

with

daily_disp as (

    select * from {{ ref('stg_procount__completiondailydisp') }}

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
        d.completiondailydisp_sk,
        d.merrick_id,
        d.record_date,
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

        -- volumes
        d.alloc_est_oil_vol,
        d.alloc_est_gas_vol_mcf,
        d.alloc_est_gas_vol_mmbtu,
        d.alloc_est_water_vol,
        d.alloc_est_ngl_vol,
        d.alloc_est_other_vol,
        d.alloc_est_co2_vol,
        d.alloc_est_mass,

        -- injection volumes
        d.alloc_est_inj_oil_vol,
        d.alloc_est_inj_gas_vol_mcf,
        d.alloc_est_inj_water_vol,
        d.alloc_est_inj_co2_vol,
        d.alloc_est_inj_other_vol,

        -- plant gas
        d.alloc_est_plant_gas_mcf,
        d.alloc_est_plant_gas_mmbtu,

        -- quality
        d.alloc_est_gravity,
        d.alloc_est_btu_factor,
        d.alloc_est_pressure_base,
        d.alloc_est_temperature,

        -- boe calculation (no cond in this table)
        coalesce(d.alloc_est_oil_vol, 0) 
            + coalesce(d.alloc_est_ngl_vol, 0) 
            + (coalesce(d.alloc_est_gas_vol_mcf, 0) / 6.0) as alloc_boe,

        -- flags
        d.processed_flag,
        d.run_ticket_flag,
        d.overwrite_flag,
        d.alloc_est_wet_dry_flag,

        -- dates
        d.run_ticket_date,
        d.allocation_date_stamp,

        -- metadata
        d._fivetran_synced,
        d._loaded_at

    from daily_disp d
    left join completions c
        on d.merrick_id = c.merrick_id

),

final as (

    select * from enriched

)

select * from final
