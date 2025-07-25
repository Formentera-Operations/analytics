{{ config(
    materialized='view',
    tags=['prodview', 'routes', 'security', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVROUTESETROUTEUSERID') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as route_userid_id,
        idrecparent as route_id,
        idflownet as flow_network_id,
        
        -- User assignment
        userid as user_id,
        typ as assignment_type,
        
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