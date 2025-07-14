{{ config(
    materialized='view',
    tags=['wellview', 'wellbore', 'directional', 'survey', 'data', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVWELLBOREDIRSURVEYDATA') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idwell,
        idrecparent,
        idrec,
        
        -- Basic survey information
        annotation,
        
        -- Angular measurements (no conversion - remain as degrees)
        azimuth,
        inclination,
        tfograv,
        tfomag,
        
        -- Rate measurements (converted to degrees per 100 feet)
        buildratecalc / 0.0328083989501312 as buildratecalc,
        dlscalc / 0.0328083989501312 as dlscalc,
        dlsoverride / 0.0328083989501312 as dlsoverride,
        turnratecalc / 0.0328083989501312 as turnratecalc,
        
        -- Calculation and override flags
        calcoverride,
        correction,
        
        -- Distance/depth measurements (converted from meters to feet)
        departcalc / 0.3048 as departcalc,
        displaceunwrapcalc / 0.3048 as displaceunwrapcalc,
        ewcalc / 0.3048 as ewcalc,
        ewoverride / 0.3048 as ewoverride,
        md / 0.3048 as md,
        nscalc / 0.3048 as nscalc,
        nsoverride / 0.3048 as nsoverride,
        tvdcalc / 0.3048 as tvdcalc,
        tvdoverride / 0.3048 as tvdoverride,
        tvdsscalc / 0.3048 as tvdsscalc,
        vscalc / 0.3048 as vscalc,
        vsoverride / 0.3048 as vsoverride,
        
        -- Quality control flags
        dontuse,
        dontusereason,
        
        -- Survey timing
        dttm,
        
        -- Gravity measurements (converted from m/s² to ft/s²)
        gravaxialraw / 0.3048 as gravaxialraw,
        gravtran1raw / 0.3048 as gravtran1raw,
        gravtran2raw / 0.3048 as gravtran2raw,
        
        -- Geographic coordinates (no conversion)
        latitude,
        longitude,
        latlongsource,
        
        -- Magnetic measurements (converted to nanotesla)
        magaxialraw / 1E-09 as magaxialraw,
        magtran1raw / 1E-09 as magtran1raw,
        magtran2raw / 1E-09 as magtran2raw,
        
        -- Survey metadata
        model,
        note,
        source,
        surveyedby,
        surveymethod,
        
        -- Tool information
        tooltyp1,
        tooltyp2,
        
        -- UTM coordinates (no conversion - remain in meters)
        utmgridzone,
        utmsource,
        utmx,
        utmy,
        
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