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
        idrec as "Record ID",
        idrecparent as "Parent Record ID",
        idflownet as "Flow Net ID",
        
        -- Route information
        name as "Route Name",
        com as Notes,
        
        -- User-defined fields
        usertxt1 as "Foreman",
        usertxt2 as "Primary Lease Operator",
        usertxt3 as "Backup Lease Operator",
        
        -- System fields
        syscreatedate as "Created Date (UTC)",
        syscreateuser as "Created By",
        sysmoddate as "Last Modified Date (UTC)",
        sysmoduser as "Last Modified By",
        systag as "Record Tag",
        syslockdate as "Lock Date (UTC)",
        syslockme as "Lock Me",
        syslockchildren as "Lock My Children",
        syslockmeui as "Lock Me (UI)",
        syslockchildrenui as "Lock My Children (UI)",
        
        -- Fivetran fields
        _fivetran_synced as fivetran_synced_at

    from source_data
)

select * from renamed