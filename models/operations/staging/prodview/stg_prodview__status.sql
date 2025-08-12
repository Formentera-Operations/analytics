{{ config(
    materialized='view',
    tags=['prodview', 'status', 'completions', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPSTATUS') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as "Status Record ID",
        idrecparent as "Status Parent Record ID",
        idflownet as "Flow Net ID",

        -- Status information
        dttm as "Status Date",
        status as "Status",

        -- Fluid and flow characteristics
        primaryfluidtyp as "Primary Fluid Type",
        flowdirection as "Flow Direction",
        commingled as "Commingled",
        typfluidprod as "Oil Or Condensate",

        -- Completion characteristics
        typcompletion as "Completion Type",
        methodprod as "Production Method",

        -- Calculation and reporting flags
        calclostprod as "Calculate Lost Production",
        wellcountincl as "Include In Well Count",

        -- User-defined fields - Text
        usertxt1 as "User Text 1",
        usertxt2 as "User Text 2",
        usertxt3 as "User Text 3",

        -- User-defined fields - Numeric
        usernum1 as "User Num 1",
        usernum2 as "User Num 2",
        usernum3 as "User Num 3",

        -- Comments
        com as "Comment",

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