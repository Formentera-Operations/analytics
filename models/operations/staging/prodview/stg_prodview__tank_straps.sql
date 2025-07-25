{{ config(
    materialized='view',
    tags=['prodview', 'tanks', 'strapping', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITTANKSTRAP') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as tank_strap_id,
        idrecparent as tank_id,
        idflownet as flow_network_id,
        
        -- Strap information
        dttm as effective_date,
        name as strap_name,
        uomincrement as increment_height_uom,
        
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