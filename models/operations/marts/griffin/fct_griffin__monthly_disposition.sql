{{
    config(
        materialized='incremental',
        unique_key='completionmonthlydisp_sk',
        tags=['griffin', 'marts', 'crescent']
    )
}}

/*
    Monthly disposition fact for Griffin namespace.
    Detailed breakdown of where production volumes went (sales, fuel, flare, etc).
*/

with

disposition as (

    select * from {{ ref('int_griffin__monthly_disposition') }}
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
        d.completionmonthlydisp_sk,
        c.completion_key,
        d.merrick_id,
        d.record_date,
        d.production_month,

        -- ticket reference
        d.run_ticket_number,
        d.source_id,
        d.source_type,
        d.gathering_system_id,

        -- completion context
        d.well_name,
        d.completion_name,
        d.wellpluscompletion_name,
        d.route_name,
        d.property_number,

        -- disposition classification
        d.disposition_code,
        d.product_code,
        d.product_type,

        -- volumes
        d.alloc_oil_vol as oil_bbls,
        d.alloc_gas_vol_mcf as gas_mcf,
        d.alloc_gas_vol_mmbtu as gas_mmbtu,
        d.alloc_water_vol as water_bbls,
        d.alloc_ngl_vol as ngl_bbls,
        d.alloc_other_vol as other_vol,
        d.alloc_boe as boe,

        -- injection volumes
        d.alloc_inj_oil_vol as inj_oil_bbls,
        d.alloc_inj_gas_vol_mcf as inj_gas_mcf,
        d.alloc_inj_water_vol as inj_water_bbls,

        -- plant gas
        d.alloc_plant_gas_mcf as plant_gas_mcf,
        d.alloc_plant_gas_mmbtu as plant_gas_mmbtu,

        -- quality
        d.alloc_gravity as gravity,
        d.alloc_btu_factor as btu_factor,

        -- dates
        d.run_ticket_date,
        d.allocation_date_stamp,

        -- flags
        d.run_ticket_flag,
        d.overwrite_flag,

        -- metadata
        d._fivetran_synced,
        d._loaded_at

    from disposition d
    left join completions c
        on d.merrick_id = c.merrick_id

)

select * from final
