{{ config(
    materialized='view',
    tags=['wellview', 'job', 'report', 'drilling', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBREPORT') }}
),

final as (
    select
        -- Primary identifiers
        idwell,
        idrecparent,
        idrec,
        
        -- BHA and equipment metrics
        bhatotalruncalc,
        bitrevscalc,
        
        -- Conditions
        condhole,
        condlease,
        condroad,
        condtemp / 0.555555555555556 + 32 as condtemp,  -- Convert to Fahrenheit
        condwave,
        condweather,
        condwind,
        contactcalc,
        
        -- Cost calculations
        costforecastfieldvarcalc,
        costjobsupplyamtcalc,
        costjobsupplyamtnormcalc,
        costjobsupplyamttodatecalc,
        costjobsupplyamttodtncalc,
        costmudaddcalc,
        costmudaddnormcalc,
        costmudaddtodatecalc,
        costmudaddtodatenormcalc,
        costnormforecastfieldvarcalc,
        costperdepthcalc / 3.28083989501312 as costperdepthcalc,  -- Convert to cost/ft
        costperdepthcumcalc / 3.28083989501312 as costperdepthcumcalc,  -- Convert to cost/ft
        costperdepthvarcalc / 3.28083989501312 as costperdepthvarcalc,  -- Convert to cost/ft
        costpertldurcalc / 24 as costpertldurcalc,  -- Convert to cost/hr
        costpertldurnormcalc / 24 as costpertldurnormcalc,  -- Convert to cost/hr
        costperdepthnormcalc / 3.28083989501312 as costperdepthnormcalc,  -- Convert to cost/ft
        
        -- Projected costs
        costprojectedmljobcalc,
        costprojectedminjobcalc,
        costprojectedmaxjobcalc,
        costprojectedtljobcalc,
        costprojectedmljobnormcalc,
        costprojectedminjobnormcalc,
        costprojectedmaxjobnormcalc,
        costprojectedtljobnormcalc,
        costprojectedmlphasecalc,
        costprojectedminphasecalc,
        costprojectedmaxphasecalc,
        costprojectedtlphasecalc,
        costprojectedmlphasenormcalc,
        costprojectedminphasenormcalc,
        costprojectedmaxphasenormcalc,
        costprojectedtlphasenormcalc,
        
        costtodatecalc,
        costtodatenormcalc,
        costtotalcalc,
        costtotalnormcalc,
        
        -- Days calculations (already in days)
        daysfromspudcalc,
        daysfromspudtorrcalc,
        
        -- Depth measurements (converted to feet)
        depthenddpcalc / 0.3048 as depthenddpcalc,
        depthenddpcumcalc / 0.3048 as depthenddpcumcalc,
        depthenddpnullcalc / 0.3048 as depthenddpnullcalc,
        depthnetprogressdpcalc / 0.3048 as depthnetprogressdpcalc,
        depthperdurcalc / 7.3152 as depthperdurcalc,  -- Convert to ft/hr
        depthperdurvarcalc / 7.3152 as depthperdurvarcalc,  -- Convert to ft/hr
        depthprogressdpcalc / 0.3048 as depthprogressdpcalc,
        depthrotatingcalc / 0.3048 as depthrotatingcalc,
        depthslidingcalc / 0.3048 as depthslidingcalc,
        depthstartdpcalc / 0.3048 as depthstartdpcalc,
        depthstartdpnullcalc / 0.3048 as depthstartdpnullcalc,
        depthtvdenddpcalc / 0.3048 as depthtvdenddpcalc,
        depthtvdendprojmethod,
        depthtvdstartdpcalc / 0.3048 as depthtvdstartdpcalc,
        
        -- Date/time fields
        dttmend,
        dttmprojendmljobcalc,
        dttmprojendminjobcalc,
        dttmprojendmaxjobcalc,
        dttmprojendtljobcalc,
        dttmprojendmlphasecalc,
        dttmprojendminphasecalc,
        dttmprojendmaxphasecalc,
        dttmprojendtlphasecalc,
        dttmstart,
        
        -- Duration calculations (various unit conversions)
        durationnoprobtimecalc / 0.0416666666666667 as durationnoprobtimecalc,  -- Convert to hours
        durationnoprobtimecumcalc / 0.0416666666666667 as durationnoprobtimecumcalc,  -- Convert to hours
        durationpersonnelotcalc / 0.0416666666666667 as durationpersonnelotcalc,  -- Convert to hours
        durationpersonnelregcalc / 0.0416666666666667 as durationpersonnelregcalc,  -- Convert to hours
        durationpersonneltotcalc / 0.0416666666666667 as durationpersonneltotcalc,  -- Convert to hours
        durationproblemtimecalc / 0.0416666666666667 as durationproblemtimecalc,  -- Convert to hours
        durationproblemtimecumcalc / 0.0416666666666667 as durationproblemtimecumcalc,  -- Convert to hours
        durationsinceltinc,  -- Already in days
        durationsincerptinc,  -- Already in days
        durationtimelogcum12hrcalc,  -- Already in days
        durationtimelogcumspudcalc,  -- Already in days
        durationtimelogcumspudrrcalc,  -- Already in days
        durationtimelogtotalcalc / 0.0416666666666667 as durationtimelogtotalcalc,  -- Convert to hours
        durationtimelogtotcumcalc,  -- Already in days
        durlastsinccalc,  -- Already in days
        durlastsincreportcalc,  -- Already in days
        durlastsincrptdaycalc,  -- Already in days
        durlastsincreportrptdaycalc,  -- Already in days
        durnoprobtimecumdayscalc,  -- Already in days
        duroffbtmcalc / 0.000694444444444444 as duroffbtmcalc,  -- Convert to minutes
        duronbtmcalc / 0.000694444444444444 as duronbtmcalc,  -- Convert to minutes
        durpipemovingcalc / 0.000694444444444444 as durpipemovingcalc,  -- Convert to minutes
        durpersonnelotcumcalc / 0.0416666666666667 as durpersonnelotcumcalc,  -- Convert to hours
        durpersonnelregcumcalc / 0.0416666666666667 as durpersonnelregcumcalc,  -- Convert to hours
        durpersonneltotcumcalc / 0.0416666666666667 as durpersonneltotcumcalc,  -- Convert to hours
        durproblemtimecumdayscalc,  -- Already in days
        
        -- Projected durations (already in days)
        durprojectedmaxjobcalc,
        durprojectedmaxphasecalc,
        durprojectedminjobcalc,
        durprojectedminphasecalc,
        durprojectedmljobcalc,
        durprojectedmlphasecalc,
        durprojectedtljobcalc,
        durprojectedtlphasecalc,
        
        durstarttoendcalc,  -- Already in days
        
        -- Gas readings (converted to percentages)
        gasbackgroundavg / 0.01 as gasbackgroundavg,
        gasbackgroundmax / 0.01 as gasbackgroundmax,
        gasconnectionavg / 0.01 as gasconnectionavg,
        gasconnectionmax / 0.01 as gasconnectionmax,
        gasdrillavg / 0.01 as gasdrillavg,
        gasdrillmax / 0.01 as gasdrillmax,
        gastripavg / 0.01 as gastripavg,
        gastripmax / 0.01 as gastripmax,
        
        -- Hazard and safety
        hazardidnorptcalc,
        hazardidnorptcumcalc,
        h2smax / 1e-06 as h2smax,  -- Convert to PPM
        headcountcalc,
        
        -- Job references
        idrecjobprogramphasecalc,
        idrecjobprogramphasecalctk,
        idreclastcascalc,
        idreclastcascalctk,
        idreclastrigcalc,
        idreclastrigcalctk,
        idrecnextcas,
        idrecnextcastk,
        idrecwellborecalc,
        idrecwellborecalctk,
        
        -- Interval calculations
        intlessoncalc,
        intproblemcalc,
        
        -- Mud properties and costs
        lastmuddensitycalc / 119.826428404623 as lastmuddensitycalc,  -- Convert to lb/gal
        mudcostperdepthcalc / 3.28083989501312 as mudcostperdepthcalc,  -- Convert to cost/ft
        mudcostperdepthcumcalc / 3.28083989501312 as mudcostperdepthcumcalc,  -- Convert to cost/ft
        mudcostperdepthcumnormcalc / 3.28083989501312 as mudcostperdepthcumnormcalc,  -- Convert to cost/ft
        mudcostperdepthnormcalc / 3.28083989501312 as mudcostperdepthnormcalc,  -- Convert to cost/ft
        
        -- Percentage calculations (converted to percentages)
        pctproblemtimecalc / 0.01 as pctproblemtimecalc,
        pctproblemtimecumcalc / 0.01 as pctproblemtimecumcalc,
        percentfieldafecalc / 0.01 as percentfieldafecalc,
        percentcompletemljobcalc / 0.01 as percentcompletemljobcalc,
        percentcompleteminjobcalc / 0.01 as percentcompleteminjobcalc,
        percentcompletemaxjobcalc / 0.01 as percentcompletemaxjobcalc,
        percentcompletetljobcalc / 0.01 as percentcompletetljobcalc,
        percentcompletemlphasecalc / 0.01 as percentcompletemlphasecalc,
        percentcompleteminphasecalc / 0.01 as percentcompleteminphasecalc,
        percentcompletemaxphasecalc / 0.01 as percentcompletemaxphasecalc,
        percentcompletetlphasecalc / 0.01 as percentcompletetlphasecalc,
        percentdepthrotatingcalc / 0.01 as percentdepthrotatingcalc,
        percentdepthslidingcalc / 0.01 as percentdepthslidingcalc,
        percenttmrotatingcalc / 0.01 as percenttmrotatingcalc,
        percenttmslidingcalc / 0.01 as percenttmslidingcalc,
        
        plannextrptops,
        
        -- Ratio calculations (converted to percentages)
        ratiodurprojmlplancalc / 0.01 as ratiodurprojmlplancalc,
        ratiodurprojminplancalc / 0.01 as ratiodurprojminplancalc,
        ratiodurprojmaxplancalc / 0.01 as ratiodurprojmaxplancalc,
        ratiodurprojtlplancalc / 0.01 as ratiodurprojtlplancalc,
        
        remarks,
        reportdaycalc,  -- Already in days
        reportnocalc,
        rigscalc,
        rigdayscalc,  -- Already in days
        rigdayscumcalc,  -- Already in days
        rigtime / 0.0416666666666667 as rigtime,  -- Convert to hours
        rigtimecumcalc / 0.0416666666666667 as rigtimecumcalc,  -- Convert to hours
        
        -- Rate of Penetration (converted to ft/hr)
        ropcalc / 7.3152 as ropcalc,
        roprotatingcalc / 7.3152 as roprotatingcalc,
        ropslidingcalc / 7.3152 as ropslidingcalc,
        
        rpttmactops,
        
        -- Safety metrics
        safetyincnocalc,
        safetyinccalc,
        safetyincnocumcalc,
        safetyincratecalc,
        safetyincreportcalc,
        safetyincreportnocalc,
        safetyincreportnocumcalc,
        safetyincreportratecalc,
        
        statusend,
        summaryops,
        
        -- Time ahead calculations (already in days)
        timeaheadmaxphasecalc,
        timeaheadmaxjobcalc,
        timeaheadminjobcalc,
        timeaheadminphasecalc,
        timeaheadmljobcalc,
        timeaheadmlphasecalc,
        timeaheadtljobcalc,
        timeaheadtlphasecalc,
        
        -- Time log codes
        timelogcode1calc,
        timelogcode2calc,
        timelogcode3calc,
        timelogcode4calc,
        
        -- Time calculations (converted to hours)
        tmcirccalc / 0.0416666666666667 as tmcirccalc,
        tmcirccumcalc / 0.0416666666666667 as tmcirccumcalc,
        tmcirctripothercumcalc / 0.0416666666666667 as tmcirctripothercumcalc,
        tmcirctripothercalc / 0.0416666666666667 as tmcirctripothercalc,
        tmdrillcalc / 0.0416666666666667 as tmdrillcalc,
        tmdrillcumcalc / 0.0416666666666667 as tmdrillcumcalc,
        tmdrillcumnoexccalc / 0.0416666666666667 as tmdrillcumnoexccalc,
        tmdrillnoexccalc / 0.0416666666666667 as tmdrillnoexccalc,
        tmothercalc / 0.0416666666666667 as tmothercalc,
        tmothercumcalc / 0.0416666666666667 as tmothercumcalc,
        tmrotatingcalc / 0.0416666666666667 as tmrotatingcalc,
        tmslidingcalc / 0.0416666666666667 as tmslidingcalc,
        tmtripcalc / 0.0416666666666667 as tmtripcalc,
        tmtripcumcalc / 0.0416666666666667 as tmtripcumcalc,
        
        -- User fields
        userboolean1,
        userboolean2,
        usernum1,
        usernum2,
        usernum3,
        usernum4,
        usernum5,
        usertxt1,
        usertxt2,
        usertxt3,
        usertxt4,
        usertxt5,
        
        -- Volume calculations (converted to barrels)
        volbittoshoecalc / 0.158987294928 as volbittoshoecalc,
        volcastoptorisertopcalc / 0.158987294928 as volcastoptorisertopcalc,
        volpumptobitcalc / 0.158987294928 as volpumptobitcalc,
        volshoetocastopcalc / 0.158987294928 as volshoetocastopcalc,
        volholecalc / 0.158987294928 as volholecalc,
        volmudactivecalc / 0.158987294928 as volmudactivecalc,
        volmudactivevarcalc / 0.158987294928 as volmudactivevarcalc,
        volmudbalancecalc / 0.158987294928 as volmudbalancecalc,
        volholevarcalc / 0.158987294928 as volholevarcalc,
        volmudaddedcalc / 0.158987294928 as volmudaddedcalc,
        volmudaddedcumcalc / 0.158987294928 as volmudaddedcumcalc,
        volmudlosscalc / 0.158987294928 as volmudlosscalc,
        volmudlosscumcalc / 0.158987294928 as volmudlosscumcalc,
        volmudaddedlossvarcalc / 0.158987294928 as volmudaddedlossvarcalc,
        volmudaddedlossvarcumcalc / 0.158987294928 as volmudaddedlossvarcumcalc,
        volmudtankcalc / 0.158987294928 as volmudtankcalc,
        
        -- Weight calculations (converted to pounds)
        weightmetalrecovtotalcalc / 0.45359237 as weightmetalrecovtotalcalc,
        
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