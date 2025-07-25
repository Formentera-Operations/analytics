{{ config(
    materialized='view',
    tags=['prodview', 'tanks', 'inventory', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITTANKSTARTINV') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as tank_inventory_id,
        idrecparent as tank_id,
        idflownet as flow_network_id,
        
        -- Origin information
        idrecunitorigin as originating_unit_id,
        idrecunitorigintk as originating_unit_table,
        idreccomporigin as originating_completion_id,
        idreccomporigintk as originating_completion_table,
        
        -- Inventory date
        dttminv as inventory_date,
        
        -- Inventory volumes (converted to US units)
        volhcliq / 0.158987294928 as hcliq_volume_bbl,
        volgas / 28.316846592 as gas_volume_mcf,
        volwater / 0.158987294928 as water_volume_bbl,
        volsand / 0.158987294928 as sand_volume_bbl,
        
        -- Tracking flag
        keepwhole as keep_whole_flag,
        
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