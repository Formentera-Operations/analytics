{{ config(
    materialized='view',
    tags=['prodview', 'downtime', 'completions', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPDOWNTM') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as "Completion Downtime Record ID",
        idrecparent as "Completion Downtime Parent ID",
        idflownet as "Flow Net ID",
        
        -- Downtime classification
        typdowntm as "Type of Downtime Entry",
        product as Product,
        location as "Location",
        failflag as "Is failure",
        
        -- Downtime period - Start
        dttmstart as "First Day",
        durdownstartday / 0.0416666666666667 as "Hours Down",
        
        -- Downtime period - End
        dttmend as "Last Day",
        durdownendday / 0.0416666666666667 as "Downtime on Last Day",
        
        -- Total downtime calculation
        durdowncalc / 0.0416666666666667 as "Total Downtime Hours",
        
        -- Planned downtime
        dttmplanend as "Planned End Date",
        durdownplanend / 0.0416666666666667 as "Planned Downtime Duration",
        
        -- Downtime codes
        codedowntm1 as "Downtime Code",
        codedowntm2 as "Downtime Code 2",
        codedowntm3 as "Downtime Code 3",
        
        -- Comments and notes
        com as Comments,
        
        -- User-defined fields - Text
        usertxt1 as "User Text 1",
        usertxt2 as "User Text 2",
        usertxt3 as "User Text 3",
        usertxt4 as "User Text 4",
        usertxt5 as "User Text 5",

        -- User-defined fields - Numeric
        usernum1 as "User Num 1",
        usernum2 as "User Num 2",
        usernum3 as "User Num 3",
        usernum4 as "User Num 4",
        usernum5 as "User Num 5",

        -- User-defined fields - Datetime
        userdttm1 as "User Date 1",
        userdttm2 as "User Date 2",
        userdttm3 as "User Date 3",
        userdttm4 as "User Date 4",
        userdttm5 as "User Date 5",

        -- System fields
        syscreatedate as "Created At (UTC)",
        syscreateuser as "Created By",
        sysmoddate as "Last Mod At (UTC)",
        sysmoduser as "Last Mod By",
        systag as "System Tag",
        syslockdate as "System Lock Date (UTC)",
        syslockme as "System Lock Me",
        syslockchildren as "System Lock Children",
        syslockmeui as "System Lock Me UI",
        syslockchildrenui as "System Lock Children UI",

        
        -- Fivetran fields
        _fivetran_synced as "Fivetran Synced At"
        
    from source_data
)

select * from renamed