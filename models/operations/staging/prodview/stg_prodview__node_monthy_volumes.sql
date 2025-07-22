{{ config(
    materialized='view',
    tags=['prodview', 'nodes', 'calculations', 'monthly', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITNODEMONTHCALC') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as node_calculation_id,
        idrecparent as parent_unit_id,
        idflownet as flow_network_id,
        
        -- Date/Time information
        dttmstart as period_start_date,
        dttmend as period_end_date,
        year as calculation_year,
        month as calculation_month,
        
        -- Gathered volumes (converted to US units)
        volhcliq / 0.158987294928 as gathered_hcliq_bbl,
        volhcliqgaseq / 28.316846592 as gathered_gas_equivalent_hcliq_mcf,
        volgas / 28.316846592 as gathered_gas_mcf,
        volwater / 0.158987294928 as gathered_water_bbl,
        volsand / 0.158987294928 as gathered_sand_bbl,
        
        -- Heat content (converted to US units)
        heat / 1055055852.62 as gathered_heat_mmbtu,
        factheat / 37258.9458078313 as gathered_heat_factor_btu_per_ft3,
        
        -- Facility reference
        idrecfacility as current_facility_id,
        idrecfacilitytk as current_facility_table,
        
        -- System fields
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,
        syslockdate as system_lock_date,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        
        -- Fivetran fields
        _fivetran_synced as fivetran_synced_at

    from source_data
)

select * from renamed