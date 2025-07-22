{{ config(
    materialized='view',
    tags=['prodview', 'nodes', 'corrections', 'daily', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITNODECORRDAY') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as node_correction_id,
        idrecparent as parent_node_id,
        idflownet as flow_network_id,
        
        -- Date/Time information
        dttm as correction_date,
        
        -- Volume corrections (converted to US units)
        volhcliq / 0.158987294928 as final_hcliq_bbl,
        volgas / 28.316846592 as final_gas_mcf,
        volwater / 0.158987294928 as final_water_bbl,
        volsand / 0.158987294928 as final_sand_bbl,
        
        -- Heat content (converted to US units)
        heat / 1055055852.62 as final_heat_mmbtu,
        
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