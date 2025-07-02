{{
  config(
    materialized='view',
    alias='pvunitmeterliquidentry'
  )
}}

with source as (
    select * from {{ source('prodview', 'PVT_PVUNITMETERLIQUIDENTRY') }}
),

renamed as (
    select
        -- Primary identifiers
        IDFLOWNET,
        IDRECPARENT,
        IDREC,
        DTTM,
        
        -- Meter readings
        READINGEND,
        READINGSTART,
        
        -- Quality measurements (decimal to percentage)
        BSW / 0.01 as BSW,
        case when BSW is not null then '%' else null end as BSWUNITLABEL,
        SANDCUT / 0.01 as SANDCUT,
        case when SANDCUT is not null then '%' else null end as SANDCUTUNITLABEL,
        
        -- General information
        COM,
        REASONOR,
        
        -- Uncorrected volumes (cubic meters to barrels)
        VOLUNCORRTOTALCALC / 0.158987294928 as VOLUNCORRTOTALCALC,
        case when VOLUNCORRTOTALCALC is not null then 'BBL' else null end as VOLUNCORRTOTALCALCUNITLABEL,
        VOLUNCORRHCLIQCALC / 0.158987294928 as VOLUNCORRHCLIQCALC,
        case when VOLUNCORRHCLIQCALC is not null then 'BBL' else null end as VOLUNCORRHCLIQCALCUNITLABEL,
        
        -- Sample conditions (temperature: Celsius to Fahrenheit, pressure: kPa to PSI)
        TEMPOFVOL / 0.555555555555556 + 32 as TEMPOFVOL,
        case when TEMPOFVOL is not null then '°F' else null end as TEMPOFVOLUNITLABEL,
        PRESOFVOL / 6.894757 as PRESOFVOL,
        case when PRESOFVOL is not null then 'PSI' else null end as PRESOFVOLUNITLABEL,
        TEMPSAMPLE / 0.555555555555556 + 32 as TEMPSAMPLE,
        case when TEMPSAMPLE is not null then '°F' else null end as TEMPSAMPLEUNITLABEL,
        PRESSAMPLE / 6.894757 as PRESSAMPLE,
        case when PRESSAMPLE is not null then 'PSI' else null end as PRESSAMPLEUNITLABEL,
        
        -- Density measurements (complex formula to API gravity)
        power(nullif(DENSITYSAMPLE, 0), -1) / 7.07409872233005E-06 + -131.5 as DENSITYSAMPLE,
        case when DENSITYSAMPLE is not null then '°API' else null end as DENSITYSAMPLEUNITLABEL,
        power(nullif(DENSITYSAMPLE60F, 0), -1) / 7.07409872233005E-06 + -131.5 as DENSITYSAMPLE60F,
        case when DENSITYSAMPLE60F is not null then '°API' else null end as DENSITYSAMPLE60FUNITLABEL,
        
        -- Corrected volumes
        VOLCORRTOTALCALC / 0.158987294928 as VOLCORRTOTALCALC,
        case when VOLCORRTOTALCALC is not null then 'BBL' else null end as VOLCORRTOTALCALCUNITLABEL,
        VOLCORRHCLIQCALC / 0.158987294928 as VOLCORRHCLIQCALC,
        case when VOLCORRHCLIQCALC is not null then 'BBL' else null end as VOLCORRHCLIQCALCUNITLABEL,
        
        -- Corrected quality measurements
        BSWCORRCALC / 0.01 as BSWCORRCALC,
        case when BSWCORRCALC is not null then '%' else null end as BSWCORRCALCUNITLABEL,
        SANDCUTCORRCALC / 0.01 as SANDCUTCORRCALC,
        case when SANDCUTCORRCALC is not null then '%' else null end as SANDCUTCORRCALCUNITLABEL,
        
        -- Override conditions
        TEMPOR / 0.555555555555556 + 32 as TEMPOR,
        case when TEMPOR is not null then '°F' else null end as TEMPORUNITLABEL,
        PRESOR / 6.894757 as PRESOR,
        case when PRESOR is not null then 'PSI' else null end as PRESORUNITLABEL,
        power(nullif(DENSITYOR, 0), -1) / 7.07409872233005E-06 + -131.5 as DENSITYOR,
        case when DENSITYOR is not null then '°API' else null end as DENSITYORUNITLABEL,
        
        -- Reference and tracking
        REFID,
        ORIGSTATEMENTID,
        SOURCE,
        VERIFIED,
        
        -- Override volumes
        VOLORHCLIQ / 0.158987294928 as VOLORHCLIQ,
        case when VOLORHCLIQ is not null then 'BBL' else null end as VOLORHCLIQUNITLABEL,
        VOLORWATER / 0.158987294928 as VOLORWATER,
        case when VOLORWATER is not null then 'BBL' else null end as VOLORWATERUNITLABEL,
        VOLORSAND / 0.158987294928 as VOLORSAND,
        case when VOLORSAND is not null then 'BBL' else null end as VOLORSANDUNITLABEL,
        
        -- Final calculated volumes
        VOLTOTALCALC / 0.158987294928 as VOLTOTALCALC,
        case when VOLTOTALCALC is not null then 'BBL' else null end as VOLTOTALCALCUNITLABEL,
        VOLHCLIQCALC / 0.158987294928 as VOLHCLIQCALC,
        case when VOLHCLIQCALC is not null then 'BBL' else null end as VOLHCLIQCALCUNITLABEL,
        VOLHCLIQGASEQCALC / 28.316846592 as VOLHCLIQGASEQCALC,
        case when VOLHCLIQGASEQCALC is not null then 'MCF' else null end as VOLHCLIQGASEQCALCUNITLABEL,
        VOLWATERCALC / 0.158987294928 as VOLWATERCALC,
        case when VOLWATERCALC is not null then 'BBL' else null end as VOLWATERCALCUNITLABEL,
        VOLSANDCALC / 0.158987294928 as VOLSANDCALC,
        case when VOLSANDCALC is not null then 'BBL' else null end as VOLSANDCALCUNITLABEL,
        
        -- Final calculated quality
        BSWCALC / 0.01 as BSWCALC,
        case when BSWCALC is not null then '%' else null end as BSWCALCUNITLABEL,
        SANDCUTCALC / 0.01 as SANDCUTCALC,
        case when SANDCUTCALC is not null then '%' else null end as SANDCUTCALCUNITLABEL,
        
        -- Ticket information
        TICKETNO,
        TICKETSUBNO,
        
        -- Analysis and seal references
        IDRECHCLIQANALYSISCALC,
        IDRECHCLIQANALYSISCALCTK,
        IDRECSEALENTRY,
        IDRECSEALENTRYTK,
        
        -- User-defined text fields
        USERTXT1,
        USERTXT2,
        USERTXT3,
        
        -- User-defined numeric fields
        USERNUM1,
        USERNUM2,
        USERNUM3,
        
        -- User-defined datetime fields
        USERDTTM1,
        USERDTTM2,
        USERDTTM3,
        
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