{{
  config(
    materialized='view'
  )
}}

with source as (

    select * from {{ source('prodview', 'PVT_PVUNITCOMPTEST') }}

),

renamed as (

    select
        -- Primary identifiers
        idflownet,
        idrecparent,
        idrec,
        
        -- Test information
        typ,
        idrecsep,
        idrecseptk,
        durtest / 0.0416666666666667 as durtest,           -- Convert days to hours
        dontuse,
        idrecsepunitcalc,
        idrecsepunitcalctk,
        com,
        dttm,
        
        -- Test conditions
        szchoke / 0.000396875 as szchoke,                  -- Convert meters to 1/64"
        
        -- Oil/Emulsion measurements
        readinghcliqstart,
        readinghcliqend,
        bsw / 0.01 as bsw,                                 -- Convert proportion to %
        sandcut / 0.01 as sandcut,                         -- Convert proportion to %
        leveltankstart,
        leveltankend,
        leveltankfreewaterstart,
        leveltankfreewaterend,
        voltankstartcalc / 0.158987294928 as voltankstartcalc,      -- Convert m³ to BBL
        voltankendcalc / 0.158987294928 as voltankendcalc,          -- Convert m³ to BBL
        volenterhcliq / 0.158987294928 as volenterhcliq,            -- Convert m³ to BBL
        volhcliq / 0.158987294928 as volhcliq,                      -- Convert m³ to BBL
        factgasinsoln / 178.107606679035 as factgasinsoln,         -- Convert m³/m³ to MCF/BBL
        
        -- Gas measurements
        duronor / 0.0416666666666667 as duronor,           -- Convert days to hours
        presgasstatic,                                      -- Keep as-is
        presgasdiff,                                        -- Keep as-is
        tempgas,                                            -- Keep as-is
        szorifice,                                          -- Keep as-is
        cprime,
        volentergas / 28.316846592 as volentergas,          -- Convert m³ to MCF
        volgas / 28.316846592 as volgas,                    -- Convert m³ to MCF
        
        -- Water measurements
        readingwaterstart,                                  -- Keep as-is
        readingwaterend,                                    -- Keep as-is
        volenterwater / 0.158987294928 as volenterwater,    -- Convert m³ to BBL
        volwater / 0.158987294928 as volwater,              -- Convert m³ to BBL
        
        -- Sand measurements
        volentersand / 0.158987294928 as volentersand,      -- Convert m³ to BBL
        volsand / 0.158987294928 as volsand,                -- Convert m³ to BBL
        
        -- Calculated volumes
        volfluidtotalcalc / 0.158987294928 as volfluidtotalcalc,    -- Convert m³ to BBL
        volhcliqcalc / 0.158987294928 as volhcliqcalc,              -- Convert m³ to BBL
        volbeforetpcorrhcliqcalc / 0.158987294928 as volbeforetpcorrhcliqcalc,  -- Convert m³ to BBL
        volhcliqgaseqcalc / 28.316846592 as volhcliqgaseqcalc,      -- Convert m³ to MCF
        volwatercalc / 0.158987294928 as volwatercalc,              -- Convert m³ to BBL
        volgascalc / 28.316846592 as volgascalc,                    -- Convert m³ to MCF
        volliftgasrecov / 28.316846592 as volliftgasrecov,          -- Convert m³ to MCF
        volsandcalc / 0.158987294928 as volsandcalc,                -- Convert m³ to BBL
        
        -- Calculated rates
        ratefluidtotalcalc / 0.1589873 as ratefluidtotalcalc,       -- Convert m³/day to BBL/day
        ratehcliqcalc / 0.1589873 as ratehcliqcalc,                 -- Convert m³/day to BBL/day
        ratehcliqgaseqcalc / 28.316846592 as ratehcliqgaseqcalc,    -- Convert m³/day to MCF/day
        ratetotalgascalc / 28.316846592 as ratetotalgascalc,        -- Convert m³/day to MCF/day
        rateliftgascalc / 28.316846592 as rateliftgascalc,          -- Convert m³/day to MCF/day
        rateprodgascalc / 28.316846592 as rateprodgascalc,          -- Convert m³/day to MCF/day
        ratewatercalc / 0.1589873 as ratewatercalc,                 -- Convert m³/day to BBL/day
        ratesandcalc / 0.1589873 as ratesandcalc,                   -- Convert m³/day to BBL/day
        
        -- Calculated totals
        bswtotalcalc / 0.01 as bswtotalcalc,                        -- Convert proportion to %
        sandcuttotalcalc / 0.01 as sandcuttotalcalc,                -- Convert proportion to %
        gorcalc / 178.107606679035 as gorcalc,                      -- Convert m³/m³ to MCF/BBL
        cgrcalc / 0.00561458333333333 as cgrcalc,                   -- Convert m³/m³ to BBL/MCF
        wgrcalc / 0.00561458333333333 as wgrcalc,                   -- Convert m³/m³ to BBL/MCF
        
        -- Change calculations
        ratechghcliqcalc / 0.1589873 as ratechghcliqcalc,           -- Convert m³/day to BBL/day
        pctchghcliqcalc / 0.01 as pctchghcliqcalc,                  -- Convert proportion to %
        ratechggascalc / 28.316846592 as ratechggascalc,            -- Convert m³/day to MCF/day
        pctchggascalc / 0.01 as pctchggascalc,                      -- Convert proportion to %
        ratechgwatercalc / 0.1589873 as ratechgwatercalc,           -- Convert m³/day to BBL/day
        pctchgwatercalc / 0.01 as pctchgwatercalc,                  -- Convert proportion to %
        ratechgsandcalc / 0.1589873 as ratechgsandcalc,             -- Convert m³/day to BBL/day
        pctchgsandcalc / 0.01 as pctchgsandcalc,                    -- Convert proportion to %
        chggorcalc / 178.107606679035 as chggorcalc,                -- Convert m³/m³ to MCF/BBL
        pctchggorcalc / 0.01 as pctchggorcalc,                      -- Convert proportion to %
        chgcgrcalc / 0.00561458333333333 as chgcgrcalc,             -- Convert m³/m³ to BBL/MCF
        pctchgcgrcalc / 0.01 as pctchgcgrcalc,                      -- Convert proportion to %
        chgwgrcalc / 0.00561458333333333 as chgwgrcalc,             -- Convert m³/m³ to BBL/MCF
        pctchgwgrcalc / 0.01 as pctchgwgrcalc,                      -- Convert proportion to %
        chgbswcalc / 0.01 as chgbswcalc,                            -- Convert proportion to %
        pctchgbswcalc / 0.01 as pctchgbswcalc,                      -- Convert proportion to %
        chgsandcutcalc / 0.01 as chgsandcutcalc,                    -- Convert proportion to %
        pctchgsandcutcalc / 0.01 as pctchgsandcutcalc,              -- Convert proportion to %
        
        -- Reference information
        reasonvariance,
        datasource,
        
        -- Pressure conditions (convert from kPa to PSI)
        presbh / 6.894757 as presbh,                               -- Convert kPa to PSI
        prescas / 6.894757 as prescas,                             -- Convert kPa to PSI
        presinjectgasliftgas / 6.894757 as presinjectgasliftgas,   -- Convert kPa to PSI
        presprodsep / 6.894757 as presprodsep,                     -- Convert kPa to PSI
        prestestsep / 6.894757 as prestestsep,                     -- Convert kPa to PSI
        preswh / 6.894757 as preswh,                               -- Convert kPa to PSI
        
        -- Test purposes
        purposealloc,
        purposedeliv,
        purposereg,
        
        -- Temperature conditions (convert from °C to °F)
        tempbh / 0.555555555555556 + 32 as tempbh,                 -- Convert °C to °F
        tempprodsep / 0.555555555555556 + 32 as tempprodsep,       -- Convert °C to °F
        temptestsep / 0.555555555555556 + 32 as temptestsep,       -- Convert °C to °F
        tempwh / 0.555555555555556 + 32 as tempwh,                 -- Convert °C to °F
        
        -- Test personnel
        testedby,
        
        -- Temperature and pressure correction
        tempstart / 0.555555555555556 + 32 as tempstart,           -- Convert °C to °F
        presstart / 6.894757 as presstart,                         -- Convert kPa to PSI
        tempend / 0.555555555555556 + 32 as tempend,               -- Convert °C to °F
        presend / 6.894757 as presend,                             -- Convert kPa to PSI
        tempsample / 0.555555555555556 + 32 as tempsample,         -- Convert °C to °F
        pressample / 6.894757 as pressample,                       -- Convert kPa to PSI
        
        -- Density measurements (convert to API gravity)
        power(nullif(densitysample, 0), -1) / 7.07409872233005e-06 - 131.5 as densitysample,    -- Convert to °API
        power(nullif(densitysample60f, 0), -1) / 7.07409872233005e-06 - 131.5 as densitysample60f,  -- Convert to °API
        
        -- Regulatory properties
        preswhsi / 6.894757 as preswhsi,                           -- Convert kPa to PSI
        preswhflow / 6.894757 as preswhflow,                       -- Convert kPa to PSI
        presbradenhead / 6.894757 as presbradenhead,               -- Convert kPa to PSI
        reasonbradenhead,
        densityrelgas,                                              -- Keep as-is
        power(nullif(densitycond, 0), -1) / 7.07409872233005e-06 - 131.5 as densitycond,  -- Convert to °API
        
        -- Other fields
        trailerduration,                                            -- Keep as-is (days)
        
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