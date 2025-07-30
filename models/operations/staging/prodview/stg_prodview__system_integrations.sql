{{ config(
    materialized='view',
    tags=['prodview', 'integration', 'system', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVSYSINTEGRATION') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as "System Integration Record ID",
        idrecparent as "System Integration Parent Record ID",
        idflownet as "Flow Net ID",
        tblkeyparent as "System Integration Table Key",
        
        -- Integration system information
        integratordes as "Integrator Description",
        integratorver as "Integrator Version",
        afproduct as "Product Description",
        
        -- External system references
        afidentity as "AF ID Entity",
        afidrec as "AF ID Rec",
        
        -- Notes and documentation
        note as "Note",
        
        -- System fields
        syscreatedate as "Create Date (UTC)",
        syscreateuser as "Created By",
        sysmoddate as "Last Mod Date (UTC)",
        sysmoduser as "Last Mod By",
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