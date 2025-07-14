{{ config(
    materialized='view',
    tags=['wellview', 'wellbore', 'directional', 'survey', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVWELLBOREDIRSURVEY') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idwell,
        idrecparent,
        idrec,
        
        -- Azimuth information
        azimuthcorrection,
        azimuthnorthtyp,
        
        -- Azimuth tie-in (no conversion - remains as degrees)
        azimuthtiein,
        
        -- Calculation flag
        calcflag,
        
        -- Basic information
        com,
        
        -- Angular measurements (no conversion - remain as degrees)
        convergence,
        declination,
        
        -- Survey status
        definitive,
        depthcorrection,
        des,
        dttm,
        
        -- Distance measurements (converted from meters to feet)
        ewtiein / 0.3048 as ewtiein,
        
        -- Job references
        idrecjob,
        idrecjobtk,
        
        -- Inclination tie-in (no conversion - remains as degrees)
        inclinationtiein,
        
        -- Measured depth tie-in (converted from meters to feet)
        mdtiein / 0.3048 as mdtiein,
        
        -- Additional information
        notecorrection,
        
        -- North-South tie-in (converted from meters to feet)
        nstiein / 0.3048 as nstiein,
        
        -- Status and version
        proposedoractual,
        propversionno,
        
        -- True vertical depth tie-in (converted from meters to feet)
        tvdtiein / 0.3048 as tvdtiein,
        
        -- Validation information
        validatedbycompany,
        validatedbyname,
        validateddttm,
        
        -- Vertical section calculation (converted from meters to feet)
        vscalc / 0.3048 as vscalc,
        
        -- System fields
        syslockmeui,
        syslockchildrenui,
        syslockme,
        syslockchildren,
        syslockdate,
        sysmoddate,
        sysmoduser,
        syscreatedate,
        syscreateuser,
        systag,
        
        -- Fivetran metadata
        _fivetran_synced as updatedate,
        _fivetran_deleted as deleted

    from source_data
)

select * from renamed