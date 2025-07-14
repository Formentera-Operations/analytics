{{ config(
    materialized='view',
    tags=['wellview', 'casing', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVCAS') }}
),

final as (
    select
        -- Primary identifiers
        idwell,
        idrec,
        
        -- Casing equipment details
        centralizers,
        centralizersnotallycalc,
        centralizersstandoffavg / 0.01 as centralizersstandoffavg,  -- Convert to %
        centralizersstandoffmin / 0.01 as centralizersstandoffmin,  -- Convert to %
        com,
        compcasdimcalc,
        compcasdimszodnomcalc,
        compcaslengthcalc,
        complexityindex,
        componentscalc,
        connthrdtopcalc,
        contractor,
        
        -- Depths (converted to feet)
        depthbtm / 0.3048 as depthbtm,
        depthtopcalc / 0.3048 as depthtopcalc,
        depthcutpull / 0.3048 as depthcutpull,
        depthonbtmtopickupcalc / 7.3152 as depthonbtmtopickupcalc,  -- Convert to FT/HR
        depthtvdbtmcalc / 0.3048 as depthtvdbtmcalc,
        depthtvdcutpullcalc / 0.3048 as depthtvdcutpullcalc,
        
        des,
        
        -- Date/time fields
        dttmcutpull,
        dttmonbottom,
        dttmoutofhole,
        dttmpickup,
        dttmproppull,
        dttmpropcutpull,
        dttmpull,
        dttmrun,
        
        -- Duration calculations
        duronbottomtopickupcalc / 0.0416666666666667 as duronbottomtopickupcalc,  -- Convert to hours
        durruntopullcalc,  -- Already in days
        
        gradecalc,
        
        -- Job references
        idrecjobpull,
        idrecjobpulltk,
        idrecjobrun,
        idrecjobruntk,
        idrecjobprogramphasecalc,
        idrecjobprogramphasecalctk,
        idreclastrigcalc,
        idreclastrigcalctk,
        idreclastfailurecalc,
        idreclastfailurecalctk,
        idrecwellbore,
        idrecwellboretk,
        
        latposition,
        
        -- Leak-off calculations
        leakoffdensityfluidcalc / 119.826428404623 as leakoffdensityfluidcalc,  -- Convert to LB/GAL
        leakoffprescalc / 6.894757 as leakoffprescalc,  -- Convert to PSI
        
        lengthcalc / 0.3048 as lengthcalc,  -- Convert to feet
        notecutpull,
        operatingpresslimit / 6.894757 as operatingpresslimit,  -- Convert to PSI
        proposedoractual,
        propversionno,
        pullreason,
        pullreasondetail,
        reasoncutpull,
        scratchers,
        
        stickupkbcalc / 0.3048 as stickupkbcalc,  -- Convert to feet
        
        -- String weights (converted to 1000LBF)
        stringwtdown / 4448.2216152605 as stringwtdown,
        stringwtup / 4448.2216152605 as stringwtup,
        
        -- Sizes (converted to inches)
        szdriftmincalc / 0.0254 as szdriftmincalc,
        szidnomcompmincalc / 0.0254 as szidnomcompmincalc,
        szidnommincalc / 0.0254 as szidnommincalc,
        szodnomcompmaxcalc / 0.0254 as szodnomcompmaxcalc,
        szodnommaxcalc / 0.0254 as szodnommaxcalc,
        
        tapered,
        tension / 4448.2216152605 as tension,  -- Convert to KIPS
        totalstretchsumcalc / 0.3048 as totalstretchsumcalc,  -- Convert to feet
        travelequipwt / 4448.2216152605 as travelequipwt,  -- Convert to 1000LBF
        
        -- User fields
        usertxt1,
        usertxt2,
        usertxt3,
        
        -- Volumes (converted to BBL)
        volumeinternalcalc / 0.158987294928 as volumeinternalcalc,
        volumeshoetrack / 0.158987294928 as volumeshoetrack,
        
        wellboreszcalc / 0.0254 as wellboreszcalc,  -- Convert to inches
        wtperlengthcalc / 1.48816394356955 as wtperlengthcalc,  -- Convert to LB/FT
        
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
        
        -- Special column mappings to match the view
        _fivetran_synced as updatedate,
        _fivetran_deleted as deleted
        
    from source_data
)

select * from final