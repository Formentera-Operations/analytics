{{ config(
    materialized='view',
    tags=['prodview', 'completions', 'targets', 'daily', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPTARGETDAY') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as "Target Daily Record ID",
        idrecparent as "Target Record ID",
        idflownet as "Flow Net ID",
        
        -- Date/Time information
        CAST(dttm AS DATE) as "Target Daily Date",
        
        -- Target rates for liquids (converted from m³/day to BBL/day)
        ratehcliq / 0.1589873 as "Target Daily Rate Hcliq bbl per Day",
        rateoil / 0.1589873 as "Target Daily Rate Oil bbl per Day",
        ratecond / 0.1589873 as "Target Daily Rate Condensate bbl per Day",
        ratengl / 0.1589873 as "Target Daily Rate Ngl bbl per Day",
        ratewater / 0.1589873 as "Target Daily Rate Water bbl per Day",
        ratesand / 0.1589873 as "Target Daily Rate Sand bbl per Day",

        -- Target rate for gas (converted from m³/day to MCF/day)
        rategas / 28.316846592 as "Target Daily Rate Gas mcf per Day",

        -- System fields
        syscreatedate as "Created At (UTC)",
        syscreateuser as "Created By",
        sysmoddate as "Last Mod At (UTC)",
        sysmoduser as "Last Mod By",
        systag as "System Tag",
        syslockdate as "System Lock Date",
        syslockme as "System Lock Me",
        syslockchildren as "System Lock Children",
        syslockmeui as "System Lock Me UI",
        syslockchildrenui as "System Lock Children UI",
        
        -- Fivetran fields
        _fivetran_synced as "Fivetran Synced At"

    from source_data
)

select * from renamed
