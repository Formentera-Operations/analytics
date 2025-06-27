{{
  config(
    materialized='view',
    alias='pvunitcompparam'
  )
}}

with source as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPPARAM') }}
),

renamed as (
    select
        -- Primary identifiers
        IDFLOWNET,
        IDRECPARENT,
        IDREC,
        DTTM,
        
        -- Pressure measurements (kPa to PSI)
        PRESTUB / 6.894757 as PRESTUB,
        case when PRESTUB is not null then 'PSI' else null end as PRESTUBUNITLABEL,
        PRESCAS / 6.894757 as PRESCAS,
        case when PRESCAS is not null then 'PSI' else null end as PRESCASUNITLABEL,
        PRESANNULUS / 6.894757 as PRESANNULUS,
        case when PRESANNULUS is not null then 'PSI' else null end as PRESANNULUSUNITLABEL,
        PRESLINE / 6.894757 as PRESLINE,
        case when PRESLINE is not null then 'PSI' else null end as PRESLINEUNITLABEL,
        PRESINJ / 6.894757 as PRESINJ,
        case when PRESINJ is not null then 'PSI' else null end as PRESINJUNITLABEL,
        PRESWH / 6.894757 as PRESWH,
        case when PRESWH is not null then 'PSI' else null end as PRESWHUNITLABEL,
        PRESBH / 6.894757 as PRESBH,
        case when PRESBH is not null then 'PSI' else null end as PRESBHUNITLABEL,
        PRESTUBSI / 6.894757 as PRESTUBSI,
        case when PRESTUBSI is not null then 'PSI' else null end as PRESTUBSIUNITLABEL,
        PRESCASSI / 6.894757 as PRESCASSI,
        case when PRESCASSI is not null then 'PSI' else null end as PRESCASSIUNITLABEL,
        
        -- Temperature measurements (Celsius to Fahrenheit)
        TEMPWH / 0.555555555555556 + 32 as TEMPWH,
        case when TEMPWH is not null then '°F' else null end as TEMPWHUNITLABEL,
        TEMPBH / 0.555555555555556 + 32 as TEMPBH,
        case when TEMPBH is not null then '°F' else null end as TEMPBHUNITLABEL,
        
        -- Choke size (mm to 64ths of inch)
        SZCHOKE / 0.000396875 as SZCHOKE,
        case when SZCHOKE is not null then '1/64"' else null end as SZCHOKEUNITLABEL,
        
        -- Viscosity measurements
        VISCDYNAMIC as VISCDYNAMIC,
        case when VISCDYNAMIC is not null then 'PA•S' else null end as VISCDYNAMICUNITLABEL,
        VISCKINEMATIC / 55.741824 as VISCKINEMATIC,
        case when VISCKINEMATIC is not null then 'IN²/S' else null end as VISCKINEMATICUNITLABEL,
        
        -- Chemical properties
        PH,
        case when PH is not null then 'PROPORTION' else null end as PHUNITLABEL,
        SALINITY / 1E-06 as SALINITY,
        case when SALINITY is not null then 'PPM' else null end as SALINITYUNITLABEL,
        
        -- User-defined pressure fields
        PRESUSER1 / 6.894757 as PRESUSER1,
        case when PRESUSER1 is not null then 'PSI' else null end as PRESUSER1UNITLABEL,
        PRESUSER2 / 6.894757 as PRESUSER2,
        case when PRESUSER2 is not null then 'PSI' else null end as PRESUSER2UNITLABEL,
        PRESUSER3 / 6.894757 as PRESUSER3,
        case when PRESUSER3 is not null then 'PSI' else null end as PRESUSER3UNITLABEL,
        PRESUSER4 / 6.894757 as PRESUSER4,
        case when PRESUSER4 is not null then 'PSI' else null end as PRESUSER4UNITLABEL,
        PRESUSER5 / 6.894757 as PRESUSER5,
        case when PRESUSER5 is not null then 'PSI' else null end as PRESUSER5UNITLABEL,
        
        -- User-defined temperature fields
        TEMPUSER1 / 0.555555555555556 + 32 as TEMPUSER1,
        case when TEMPUSER1 is not null then '°F' else null end as TEMPUSER1UNITLABEL,
        TEMPUSER2 / 0.555555555555556 + 32 as TEMPUSER2,
        case when TEMPUSER2 is not null then '°F' else null end as TEMPUSER2UNITLABEL,
        TEMPUSER3 / 0.555555555555556 + 32 as TEMPUSER3,
        case when TEMPUSER3 is not null then '°F' else null end as TEMPUSER3UNITLABEL,
        TEMPUSER4 / 0.555555555555556 + 32 as TEMPUSER4,
        case when TEMPUSER4 is not null then '°F' else null end as TEMPUSER4UNITLABEL,
        TEMPUSER5 / 0.555555555555556 + 32 as TEMPUSER5,
        case when TEMPUSER5 is not null then '°F' else null end as TEMPUSER5UNITLABEL,
        
        -- User-defined text fields
        USERTXT1,
        USERTXT2,
        USERTXT3,
        USERTXT4,
        USERTXT5,
        
        -- User-defined numeric fields (first two are dimensionless)
        USERNUM1,
        USERNUM2,
        -- Time conversions for user numeric fields 3-5 (seconds to minutes)
        USERNUM3 / 0.000694444444444444 as USERNUM3,
        case when USERNUM3 is not null then 'MIN' else null end as USERNUM3UNITLABEL,
        USERNUM4 / 0.000694444444444444 as USERNUM4,
        case when USERNUM4 is not null then 'MIN' else null end as USERNUM4UNITLABEL,
        USERNUM5 / 0.000694444444444444 as USERNUM5,
        case when USERNUM5 is not null then 'MIN' else null end as USERNUM5UNITLABEL,
        
        -- User-defined datetime fields
        USERDTTM1,
        USERDTTM2,
        USERDTTM3,
        USERDTTM4,
        USERDTTM5,
        
        -- General information
        COM,
        
        -- System locking fields
        SYSLOCKMEUI,
        SYSLOCKCHILDRENUI,
        SYSLOCKME,
        SYSLOCKCHILDREN,
        SYSLOCKDATE,
        
        -- System audit fields
        SYSMODDATE,
        SYSMODUSER,
        SYSCREATEDATE,
        SYSCREATEUSER,
        SYSTAG,
        
        -- Fivetran metadata
        _FIVETRAN_SYNCED as UPDATE_DATE,
        _FIVETRAN_DELETED as DELETED

    from source
)

select * from renamed