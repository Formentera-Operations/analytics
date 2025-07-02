{{ config(
    materialized='view',
    tags=['prodview', 'remarks', 'units', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITREMARK') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as remark_id,
        idrecparent as unit_id,
        idflownet as flow_network_id,
        
        -- Remark information
        dttm as entry_date,
        typ as remark_type,
        com as comments,
        
        -- Link to related item
        idrecitem as link_id,
        idrecitemtk as link_table,
        
        -- Status flags
        actionrqd as action_required,
        explainedimbalance as explains_imbalance,
        
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