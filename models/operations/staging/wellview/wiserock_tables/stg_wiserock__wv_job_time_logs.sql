{{ config(
    materialized='view',
    tags=['wellview', 'job-time-log', 'drilling', 'operations', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBTIMELOG') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idwell,
        idrecparent,
        idrec,
        
        -- Activity coding
        code1,
        code2,
        code3,
        code4,
        code1234calc,
        com,
        
        -- Time period dates
        dttmend,
        dttmstart,
        
        -- Unit conversions for depths (meters to feet)
        depthend / 0.3048 as depthend,
        depthenddpcalc / 0.3048 as depthenddpcalc,
        depthstart / 0.3048 as depthstart,
        depthstartdpcalc / 0.3048 as depthstartdpcalc,
        depthtvdendcalc / 0.3048 as depthtvdendcalc,
        depthtvdstartcalc / 0.3048 as depthtvdstartcalc,
        
        -- Unit conversions for durations (days to hours)
        durationcalc / 0.0416666666666667 as durationcalc,
        durationnoprobtimecalc / 0.0416666666666667 as durationnoprobtimecalc,
        durationproblemtimecalc / 0.0416666666666667 as durationproblemtimecalc,
        sumofdurationcalc / 0.0416666666666667 as sumofdurationcalc,
        
        -- Cumulative durations (no conversion - remain as days)
        durationnoprobtimecumcalc,
        durationproblemtimecumcalc,
        durationtimelogcumspudcalc,
        durationtimelogtotcumcalc,
        
        -- Unit conversions for short durations (days to minutes)
        duroffbtmcalc / 0.000694444444444444 as duroffbtmcalc,
        duronbtmcalc / 0.000694444444444444 as duronbtmcalc,
        durpipemovingcalc / 0.000694444444444444 as durpipemovingcalc,
        
        -- Days from spud (no conversion)
        daysfromspudcalc,
        
        -- Formation and wellbore information
        formationcalc,
        
        -- Reference IDs
        idrecjobprogramphasecalc,
        idrecjobprogramphasecalctk,
        idrecjobreportcalc,
        idrecjobreportcalctk,
        idreclastcascalc,
        idreclastcascalctk,
        idreclastintprobcalc,
        idreclastintprobcalctk,
        idreclastrigcalc,
        idreclastrigcalctk,
        idrecwellbore,
        idrecwellboretk,
        idrecwsstring,
        idrecwsstringtk,
        
        -- Status and flags
        inactive,
        
        -- Inclination measurements (no conversion - remain as degrees)
        inclendcalc,
        inclmaxcalc,
        inclstartcalc,
        
        -- Operational categories
        opscategory,
        problemcalc,
        refderrick,
        refnoproblemcalc,
        reportnocalc,
        rigcrewnamecalc,
        
        -- Rig days (no conversion)
        rigdayscalc,
        rigdayscumcalc,
        
        -- Rate of penetration (m/day to ft/hr)
        ropcalc / 7.3152 as ropcalc,
        
        -- Other operational fields
        unschedtyp,
        usertxt1,
        usertxt2,
        
        -- Wellbore size (meters to inches)
        wellboreszcalc / 0.0254 as wellboreszcalc,
        
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