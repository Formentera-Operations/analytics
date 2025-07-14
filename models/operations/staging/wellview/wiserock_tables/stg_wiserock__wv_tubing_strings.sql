{{ config(
    materialized='view',
    tags=['wellview', 'tubing', 'strings', 'completion', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVTUB') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idwell,
        idrec,
        
        -- Centralizer standoff (converted from proportion to percentage)
        centralizersnotallycalc,
        centralizersstandoffavg / 0.01 as centralizersstandoffavg,
        centralizersstandoffmin / 0.01 as centralizersstandoffmin,
        
        -- Basic information
        com,
        complexityindex,
        componentscalc,
        comptubdimcalc,
        comptubdimszodnomcalc,
        comptublengthcalc,
        connthrdtopcalc,
        contractor,
        
        -- Depths (converted from meters to feet)
        depthbtm / 0.3048 as depthbtm,
        depthbtmlinkcalc / 0.3048 as depthbtmlinkcalc,
        depthcutpull / 0.3048 as depthcutpull,
        depthtopcalc / 0.3048 as depthtopcalc,
        depthtoplinkcalc / 0.3048 as depthtoplinkcalc,
        depthtvdbtmcalc / 0.3048 as depthtvdbtmcalc,
        depthtvdcutpullcalc / 0.3048 as depthtvdcutpullcalc,
        
        -- Special depth/rate conversion (appears to be rate in ft/hr)
        depthonbtmtopickupcalc / 7.3152 as depthonbtmtopickupcalc,
        
        -- Description and dates
        des,
        dttmcutpull,
        dttmonbottom,
        dttmoutofhole,
        dttmpickup,
        dttmpropcutpull,
        dttmproppull,
        dttmpull,
        dttmrun,
        
        -- Duration conversions (days to hours for one, days remain for the other)
        duronbottomtopickupcalc / 0.0416666666666667 as duronbottomtopickupcalc,
        durruntopullcalc,
        
        -- Grade and reference IDs
        gradecalc,
        idrecjobpull,
        idrecjobpulltk,
        idrecjobprogramphasecalc,
        idrecjobprogramphasecalctk,
        idrecjobrun,
        idrecjobruntk,
        idreclastfailurecalc,
        idreclastfailurecalctk,
        idreclastrigcalc,
        idreclastrigcalctk,
        idrecstring,
        idrecstringtk,
        idrecwellbore,
        idrecwellboretk,
        
        -- Position and length
        latposition,
        lengthcalc / 0.3048 as lengthcalc,
        notecutpull,
        
        -- Operating pressure (converted from kPa to PSI)
        operatingpresslimit / 6.894757 as operatingpresslimit,
        
        -- Status and version
        proposedoractual,
        propversionno,
        pullreason,
        pullreasondetail,
        reasoncutpull,
        
        -- Stick up depth (converted from meters to feet)
        stickupkbcalc / 0.3048 as stickupkbcalc,
        
        -- String weights (converted from Newtons to 1000 lbf)
        stringwtdown / 4448.2216152605 as stringwtdown,
        stringwtrotating / 4448.2216152605 as stringwtrotating,
        stringwtup / 4448.2216152605 as stringwtup,
        
        -- Sizes (converted from meters to inches)
        szdriftmincalc / 0.0254 as szdriftmincalc,
        szidnomcompmincalc / 0.0254 as szidnomcompmincalc,
        szidnommincalc / 0.0254 as szidnommincalc,
        szodnomcompmaxcalc / 0.0254 as szodnomcompmaxcalc,
        szodnommaxcalc / 0.0254 as szodnommaxcalc,
        
        -- Physical properties
        tapered,
        
        -- Tension (converted from Newtons to lbf)
        tension / 4.4482216152605 as tension,
        
        -- Total stretch (converted from meters to feet)
        totalstretchsumcalc / 0.3048 as totalstretchsumcalc,
        
        -- User fields
        usertxt1,
        usertxt2,
        usertxt3,
        
        -- Volumes (converted from cubic meters to barrels)
        volumeinternalcalc / 0.158987294928 as volumeinternalcalc,
        volumeshoetrack / 0.158987294928 as volumeshoetrack,
        
        -- Weight per length (converted from kg/m to lb/ft)
        wtperlengthcalc / 1.48816394356955 as wtperlengthcalc,
        
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