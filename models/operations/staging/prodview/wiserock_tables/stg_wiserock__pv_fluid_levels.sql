{{
  config(
    materialized='view'
  )
}}

with source as (

    select * from {{ source('prodview', 'PVT_PVUNITCOMPFLUIDLEVEL') }}

),

renamed as (

    select
        -- Primary identifiers
        idflownet,
        idrecparent,
        idrec,
        
        -- Fluid level measurement date
        dttm,
        
        -- Joint measurements
        jointstofluid,
        jointsinhole,
        jointsoffluidcalc,
        
        -- Depth measurements (convert from meters to feet)
        depthtofluid / 0.3048 as depthtofluid,                             -- Convert m to ft
        depthfluidabovepump / 0.3048 as depthfluidabovepump,               -- Convert m to ft
        depthfluidabovepumpgasfree / 0.3048 as depthfluidabovepumpgasfree, -- Convert m to ft
        depthtopumpor / 0.3048 as depthtopumpor,                           -- Convert m to ft
        depthtopumpcalc / 0.3048 as depthtopumpcalc,                       -- Convert m to ft
        depthpumpcalc / 0.3048 as depthpumpcalc,                           -- Convert m to ft
        
        -- Rate measurements
        rateliquid / 0.1589873 as rateliquid,                              -- Convert m³/day to BBL/day
        rategas / 28.316846592 as rategas,                                 -- Convert m³/day to MCF/day
        
        -- Pressure measurements (convert from kPa to PSI)
        prescas / 6.894757 as prescas,                                     -- Convert kPa to PSI
        prestub / 6.894757 as prestub,                                     -- Convert kPa to PSI
        presbhprod / 6.894757 as presbhprod,                              -- Convert kPa to PSI
        presbhstatic / 6.894757 as presbhstatic,                          -- Convert kPa to PSI
        prespumpin / 6.894757 as prespumpin,                              -- Convert kPa to PSI
        
        -- Test information
        testedby,
        typstring,
        com,
        
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