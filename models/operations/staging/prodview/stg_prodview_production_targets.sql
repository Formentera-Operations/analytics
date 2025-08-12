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
        idrecparent as "Unit Record ID",
        idflownet as "Flow Net ID",
        
        -- Date/Time information
        CAST(dttmstart AS DATE) as "Target Start Date",
        
        -- Target Fields
        typ as "Target Type",
        usecalcdiff as "Is Use in Diff from Target Calculations",
        usertxt1 as "CC Forecast Name",
        usertxt2 as "User Text 2",
        usertxt3 as "User Text 3",

        -- System fields
        syscreatedate as "Created At (UTC)",
        syscreateuser as "Created By",
        sysmoddate as "Last Mod At (UTC)",
        sysmoduser as "Last Mod By",
        systag as "System Tag",
        
        -- Fivetran fields
        _fivetran_synced as "Fivetran Synced At"

    from source_data
)

select * from renamed
