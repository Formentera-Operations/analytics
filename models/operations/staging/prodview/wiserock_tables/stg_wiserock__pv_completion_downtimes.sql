{{
  config(
    materialized='view'
  )
}}

with source as (

    select * from {{ source('prodview', 'PVT_PVUNITCOMPDOWNTM') }}

),

renamed as (

    select
        -- Primary identifiers
        idflownet,
        idrecparent,
        idrec,
        
        -- Downtime information
        typdowntm,
        dttmstart,
        durdownstartday / 0.0416666666666667 as durdownstartday,  -- Convert days to hours
        codedowntm1,
        codedowntm2,
        codedowntm3,
        
        -- End information
        dttmend,
        durdownendday / 0.0416666666666667 as durdownendday,      -- Convert days to hours
        durdowncalc / 0.0416666666666667 as durdowncalc,          -- Convert days to hours
        
        -- Planning information
        dttmplanend,
        durdownplanend / 0.0416666666666667 as durdownplanend,    -- Convert days to hours
        
        -- Descriptive fields
        com,
        location,
        failflag,
        product,
        
        -- User-defined fields
        usertxt1,
        usertxt2,
        usertxt3,
        usertxt4,
        usertxt5,
        usernum1,
        usernum2,
        usernum3,
        usernum4,
        usernum5,
        userdttm1,
        userdttm2,
        userdttm3,
        userdttm4,
        userdttm5,
        
        -- System lock fields
        syslockmeui,
        syslockchildrenui,
        syslockme,
        syslockchildren,
        syslockdate,
        
        -- System metadata
        sysmoddate,
        sysmoduser,
        syscreatedate,
        syscreateuser,
        systag,
        
        -- Fivetran metadata mapped to standard names
        _fivetran_synced as updatedate,
        _fivetran_deleted as deleted

    from source

)

select * from renamed