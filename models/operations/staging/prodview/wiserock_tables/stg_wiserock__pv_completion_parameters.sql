{{
  config(
    materialized='view'
  )
}}

with source as (

    select * from {{ source('prodview', 'PVT_PVUNITCOMPPARAM') }}
    qualify 1 = row_number() over (partition by idrec order by _fivetran_synced desc)

),

renamed as (

    select
        -- Primary identifiers
        idflownet,
        idrecparent,
        idrec,
        
        -- Date
        dttm,
        
        -- Pressure fields (convert from kilopascals to PSI)
        prestub / 6.894757 as prestub,                    -- Convert kPa to PSI
        prescas / 6.894757 as prescas,                    -- Convert kPa to PSI
        presannulus / 6.894757 as presannulus,           -- Convert kPa to PSI
        presline / 6.894757 as presline,                 -- Convert kPa to PSI
        presinj / 6.894757 as presinj,                   -- Convert kPa to PSI
        preswh / 6.894757 as preswh,                     -- Convert kPa to PSI
        presbh / 6.894757 as presbh,                     -- Convert kPa to PSI
        prestubsi / 6.894757 as prestubsi,               -- Convert kPa to PSI
        prescassi / 6.894757 as prescassi,               -- Convert kPa to PSI
        
        -- Temperature fields (convert from Celsius to Fahrenheit)
        tempwh / 0.555555555555556 + 32 as tempwh,       -- Convert °C to °F
        tempbh / 0.555555555555556 + 32 as tempbh,       -- Convert °C to °F
        
        -- Choke size (convert from meters to 1/64")
        szchoke / 0.000396875 as szchoke,                -- Convert meters to 1/64"
        
        -- Viscosity fields
        viscdynamic,                                      -- Keep as-is (PA•S)
        visckinematic / 55.741824 as visckinematic,      -- Convert to IN²/S
        
        -- Other measurements
        ph,                                               -- Keep as-is (PROPORTION)
        salinity / 1e-06 as salinity,                    -- Convert to PPM
        
        -- User pressure fields (convert from kilopascals to PSI)
        presuser1 / 6.894757 as presuser1,               -- Convert kPa to PSI
        presuser2 / 6.894757 as presuser2,               -- Convert kPa to PSI
        presuser3 / 6.894757 as presuser3,               -- Convert kPa to PSI
        presuser4 / 6.894757 as presuser4,               -- Convert kPa to PSI
        presuser5 / 6.894757 as presuser5,               -- Convert kPa to PSI
        
        -- User temperature fields (convert from Celsius to Fahrenheit)
        tempuser1 / 0.555555555555556 + 32 as tempuser1, -- Convert °C to °F
        tempuser2 / 0.555555555555556 + 32 as tempuser2, -- Convert °C to °F
        tempuser3 / 0.555555555555556 + 32 as tempuser3, -- Convert °C to °F
        tempuser4 / 0.555555555555556 + 32 as tempuser4, -- Convert °C to °F
        tempuser5 / 0.555555555555556 + 32 as tempuser5, -- Convert °C to °F
        
        -- User-defined fields
        usertxt1,
        usertxt2,
        usertxt3,
        usertxt4,
        usertxt5,
        usernum1,
        usernum2,
        usernum3 / 0.000694444444444444 as usernum3,     -- Convert to MIN
        usernum4 / 0.000694444444444444 as usernum4,     -- Convert to MIN
        usernum5 / 0.000694444444444444 as usernum5,     -- Convert to MIN
        userdttm1,
        userdttm2,
        userdttm3,
        userdttm4,
        userdttm5,
        
        -- Comments
        com,
        
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