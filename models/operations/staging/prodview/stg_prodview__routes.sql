{{ config(
    materialized='view',
    tags=['prodview', 'routes', 'configuration', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVROUTESETROUTE') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as "Route ID",
        idrecparent as "Parent Route Set ID",
        idflownet as "Flow Network ID",
        
        -- Route information
        name as "Route Name",
        com as Notes,
        
        -- User-defined fields
        usertxt1 as "Foreman",
        usertxt2 as "Primary Lease Operator",
        usertxt3 as "Backup Lease Operator",
        
        -- System fields
        syscreatedate as "Created Date",
        syscreateuser as "Created By",
        sysmoddate as "Last Modified Date (UTC)",
        sysmoduser as "Last Modified By",
        systag as "Record Tag",
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