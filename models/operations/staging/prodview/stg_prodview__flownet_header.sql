{{ config(
    materialized='view',
    tags=['prodview', 'completions', 'targets', 'daily', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVFLOWNETHEADER') }}
    where "_FIVETRAN_DELETED" = false
),

renamed as (
    select
        -- Primary identifiers
        IDFLOWNET as "Flow Net ID",
        NAME as "Network Name",
        
        -- Date/Time information
        CAST(DTTMSTART AS DATE) as "Start Date",
        CAST(DTTMEND AS DATE) as "End Date",
        
        -- Network Fields
        TYP as "Type",
        COM as "Comment",
        IDRECUNITPRIMARY as "Primary Unit ID",
        IDRECFACILITYPRIMARY as "Primary Facility ID",
        
        -- User fields
        USERTXT1 as "User Text 1",
        USERTXT2 as "User Text 2",
        USERTXT3 as "User Text 3",
        USERTXT4 as "User Text 4",
        USERTXT5 as "User Text 5",
        
        -- System fields
        SYSCREATEDATE as "Created At (UTC)",
        SYSCREATEUSER as "Created By",
        SYSMODDATE as "Last Mod At (UTC)",
        SYSMODUSER as "Last Mod By",
        SYSTAG as "System Tag",
        
        -- Fivetran fields
        "_FIVETRAN_SYNCED" as "Fivetran Synced At"
    from source_data
)

select * from renamed