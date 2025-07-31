{{ config(
    materialized='view',
    tags=['prodview', 'completions', 'targets', 'daily', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPTARGET') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as "Target Record ID",
        idrecparent as "Target Parent ID",
        idflownet as "Flow Net ID",
        
        -- Date/Time information
        dttmstart as "Target Start Date",
        
        - - 
        -- System fields
        syscreatedate as "Created At",
        syscreateuser as "Created By",
        sysModDateLocal as "Last Mod Date"
        sysmoddate as "Last Mod At (UTC)",
        sysmoduser as "Last Mod By",
        systag as "System Tag",
        
        -- Fivetran fields
        _fivetran_synced as "Fivetran Synced At"

    from source_data
)

select * from renamed