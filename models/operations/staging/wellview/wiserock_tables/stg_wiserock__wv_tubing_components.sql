{{ config(
    materialized='view',
    tags=['wellview', 'tubing', 'components', 'completion', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVTUBCOMP') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idwell,
        idrecparent,
        idrec,
        
        -- Centralizer count
        centralizersnotallycalc,
        
        -- Coatings and basic info
        coatinginner,
        coatingouter,
        com,
        compsubtyp,
        conditionpull,
        conditionrun,
        connectaltcalc,
        connectcalc,
        
        -- Connection sizes (converted from meters to inches)
        connszbtm / 0.0254 as connszbtm,
        connsztop / 0.0254 as connsztop,
        
        -- Connection details
        connthrdbtm,
        connthrdtop,
        conntypbtm,
        conntyptop,
        
        -- Cost information
        cost,
        costunitlabel,
        
        -- Status
        currentstatus,
        currentstatuscalc,
        
        -- Depths (converted from meters to feet)
        depthbtmcalc / 0.3048 as depthbtmcalc,
        depthtopcalc / 0.3048 as depthtopcalc,
        depthtopcorrected / 0.3048 as depthtopcorrected,
        depthtvdbtmcalc / 0.3048 as depthtvdbtmcalc,
        depthtvdtopcalc / 0.3048 as depthtvdtopcalc,
        
        -- Descriptions and dates
        des,
        desjtcalc,
        dttmmanufacture,
        dttmstatuscalc,
        
        -- Fishing neck dimensions
        fishnecklength / 0.3048 as fishnecklength,
        fishneckod / 0.0254 as fishneckod,
        
        -- Material properties
        grade,
        
        -- Hours start (converted from days to hours)
        hoursstart / 0.0416666666666667 as hoursstart,
        
        -- Component details
        iconname,
        idreclastfailurecalc,
        idreclastfailurecalctk,
        
        -- Inclinations (no conversion - remain as degrees)
        inclbtmcalc,
        inclmaxcalc,
        incltopcalc,
        
        -- Item and joint information
        itemnocalc,
        joints,
        jointstallycalc,
        
        -- Lengths (converted from meters to feet)
        length / 0.3048 as length,
        lengthcumcalc / 0.3048 as lengthcumcalc,
        lengthtallycalc / 0.3048 as lengthtallycalc,
        
        -- Component attributes
        linetosurf,
        make,
        material,
        model,
        
        -- Pressures (converted from kPa to PSI)
        presaxialinner / 6.894757 as presaxialinner,
        presaxialouter / 6.894757 as presaxialouter,
        presburst / 6.894757 as presburst,
        prescollapse / 6.894757 as prescollapse,
        
        -- Additional properties
        radioactivesource,
        refid,
        sn,
        
        -- Sizes (converted from meters to inches)
        szdrift / 0.0254 as szdrift,
        szidnom / 0.0254 as szidnom,
        szodmax / 0.0254 as szodmax,
        szodnom / 0.0254 as szodnom,
        
        -- Temperature (converted from Celsius to Fahrenheit)
        temprating / 0.555555555555556 + 32 as temprating,
        
        -- Tensile strength (converted from Newtons to 1000 lbf)
        tensilemax / 4448.2216152605 as tensilemax,
        
        -- Torque values (converted from Newton-meters to ft-lb)
        torquemax / 1.3558179483314 as torquemax,
        torquemin / 1.3558179483314 as torquemin,
        
        -- Connection upsets
        upsetbtm,
        upsettop,
        usedclass,
        
        -- Volumes (converted from cubic meters to barrels)
        volumedispcalc / 0.158987294928 as volumedispcalc,
        volumedispcumcalc / 0.158987294928 as volumedispcumcalc,
        volumeinternalcalc / 0.158987294928 as volumeinternalcalc,
        
        -- Weights (different conversions for different fields)
        weightcalc / 4.4482216152605 as weightcalc,  -- Newtons to lbf
        weightcumcalc / 4448.2216152605 as weightcumcalc,  -- Newtons to 1000 lbf
        
        -- Weight per length (converted from kg/m to lb/ft)
        wtperlength / 1.48816394356955 as wtperlength,
        
        -- System fields
        sysseq,
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