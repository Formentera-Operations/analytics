{{ config(
    materialized='view',
    tags=['wellview', 'job', 'drilling', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOB') }}
),

final as (
    select
        -- Primary identifiers
        idwell,
        idrec,
        
        -- AFE (Authorization for Expenditure) calculations
        afeamtcalc,
        afeamtnormcalc,
        afenumbercalc,
        afecosttypcalc,
        afenumbersuppcalc,
        afeperdurmlcalc / 24 as afeperdurmlcalc,  -- Convert to cost/hour
        afeperdurmlnormcalc / 24 as afeperdurmlnormcalc,  -- Convert to cost/hour
        afepertargetdepthcalc / 3.28083989501312 as afepertargetdepthcalc,  -- Convert to cost/ft
        afepertargetdepthnormcalc / 3.28083989501312 as afepertargetdepthnormcalc,  -- Convert to cost/ft
        afesupamtcalc,
        afesupamtnormcalc,
        afetotalcalc,
        afetotalnormcalc,
        
        -- BHA (Bottom Hole Assembly) metrics
        bhadrillruncalc,
        bhatotalruncalc,
        bitrevscalc,
        
        -- Project and client information
        chartofaccounts,
        client,
        complexityindex,
        
        -- Cost calculations
        costafeforecastvarcalc,
        costfinalactual,
        costforecastcalc,
        costforecastfieldvarcalc,
        costmaxtotalcalc,
        costmintotalcalc,
        costmltotalcalc,
        costmltotalnoplanchangecalc,
        costmlnoexcludecalc,
        costnormafeforecastvarcalc,
        costnormforecastcalc,
        costnormforecastfieldvarcalc,
        costnormperdepthcalc / 3.28083989501312 as costnormperdepthcalc,  -- Convert to cost/ft
        costnormtotalcalc,
        costnormperlateralcalc / 3.28083989501312 as costnormperlateralcalc,  -- Convert to cost/ft
        costnormperlateralallcalc / 3.28083989501312 as costnormperlateralallcalc,  -- Convert to cost/ft
        costperdepthcalc / 3.28083989501312 as costperdepthcalc,  -- Convert to cost/ft
        costperdepthplanmlcalc / 3.28083989501312 as costperdepthplanmlcalc,  -- Convert to cost/ft
        costperlateralcalc / 3.28083989501312 as costperlateralcalc,  -- Convert to cost/ft
        costperlateralallcalc / 3.28083989501312 as costperlateralallcalc,  -- Convert to cost/ft
        costnormperdepthplanmlcalc / 3.28083989501312 as costnormperdepthplanmlcalc,  -- Convert to cost/ft
        costtechlimittotalcalc,
        costpertldurcalc / 24 as costpertldurcalc,  -- Convert to cost/hour
        costpertldurnormcalc / 24 as costpertldurnormcalc,  -- Convert to cost/hour
        costtotalcalc,
        
        -- Currency information
        currencycode,
        currencyexchangerate,
        
        -- Depth measurements (converted to feet)
        depthdrilledcalc / 0.3048 as depthdrilledcalc,
        depthdrilledperbhacalc / 0.3048 as depthdrilledperbhacalc,
        depthdrilledspudtorrcalc / 7.3152 as depthdrilledspudtorrcalc,  -- Convert to ft/hr
        depthdrilledperreportnocalc / 0.3048 as depthdrilledperreportnocalc,
        depthperdurplanmlcalc / 7.3152 as depthperdurplanmlcalc,  -- Convert to ft/hr
        depthperratiodurationcalc / 0.3048 as depthperratiodurationcalc,
        depthplanmaxcalc / 0.3048 as depthplanmaxcalc,
        depthrotatingcalc / 0.3048 as depthrotatingcalc,
        depthslidingcalc / 0.3048 as depthslidingcalc,
        
        -- Date/time fields
        dttmend,
        dttmendcalc,
        dttmendplanmaxcalc,
        dttmendplanmincalc,
        dttmendplanmlcalc,
        dttmendplantechlimitcalc,
        dttmspud,
        dttmstart,
        dttmstartplan,
        dttmtotaldepthcalc,
        
        -- Duration calculations (various unit conversions)
        durationiltcalc,  -- Already in days
        durationmaxtotalcalc,  -- Already in days
        durationmintotalcalc,  -- Already in days
        durationmltotalcalc,  -- Already in days
        durationnoproblemtimecalc / 0.0416666666666667 as durationnoproblemtimecalc,  -- Convert to hours
        durationpersonnelotcalc / 0.0416666666666667 as durationpersonnelotcalc,  -- Convert to hours
        durationpersonnelregcalc / 0.0416666666666667 as durationpersonnelregcalc,  -- Convert to hours
        durationpersonneltotcalc / 0.0416666666666667 as durationpersonneltotcalc,  -- Convert to hours
        durationproblemtimecalc / 0.0416666666666667 as durationproblemtimecalc,  -- Convert to hours
        durationspudtoplanmlcalc / 0.0416666666666667 as durationspudtoplanmlcalc,  -- Convert to hours
        durationspudtoplanmaxcalc / 0.0416666666666667 as durationspudtoplanmaxcalc,  -- Convert to hours
        durationspudtoplanmincalc / 0.0416666666666667 as durationspudtoplanmincalc,  -- Convert to hours
        durationspudtoplantechlimcalc / 0.0416666666666667 as durationspudtoplantechlimcalc,  -- Convert to hours
        durationspudtimelogcalc / 0.0416666666666667 as durationspudtimelogcalc,  -- Convert to hours
        durationspudtotdcalc / 0.0416666666666667 as durationspudtotdcalc,  -- Convert to hours
        durationspudtorrcalc / 0.0416666666666667 as durationspudtorrcalc,  -- Convert to hours
        durstarttoendcalc,  -- Already in days
        durationtechlimittotalcalc,  -- Already in days
        durationtimelogtotalcalc / 0.0416666666666667 as durationtimelogtotalcalc,  -- Convert to hours
        durmltotalnoplanchangecalc,  -- Already in days
        durmlnoexcludecalc,  -- Already in days
        duroffbtmcalc / 0.000694444444444444 as duroffbtmcalc,  -- Convert to minutes
        duronbtmcalc / 0.000694444444444444 as duronbtmcalc,  -- Convert to minutes
        durpipemovingcalc / 0.000694444444444444 as durpipemovingcalc,  -- Convert to minutes
        
        -- Estimated costs and savings
        estcostnormsavecalc,
        estcostsavecalc,
        estproblemcostcalc,
        estproblemcostnormcalc,
        estproblemtimecalc / 0.0416666666666667 as estproblemtimecalc,  -- Convert to hours
        esttimesavecalc / 0.0416666666666667 as esttimesavecalc,  -- Convert to hours
        
        -- Final invoice calculations
        finalinvoicetotalcalc,
        finalinvoicetotalnormcalc,
        
        -- Job references and identifiers
        hazardidnorptcalc,
        idreclastrigcalc,
        idreclastrigcalctk,
        idrectub,
        idrectubtk,
        idrecwellbore,
        idrecwellboretk,
        idrecwellborecalc,
        idrecwellborecalctk,
        jobida,
        jobidb,
        jobidc,
        jobsubtyp,
        jobsupplycostcalc,
        jobsupplycostnormcalc,
        jobtyp,
        
        -- Mud cost and properties
        mudcostcalc,
        mudcostnormcalc,
        mudcostperdepthcalc / 3.28083989501312 as mudcostperdepthcalc,  -- Convert to cost/ft
        mudcostperdepthnormcalc / 3.28083989501312 as mudcostperdepthnormcalc,  -- Convert to cost/ft
        muddensitymaxcalc / 119.826428404623 as muddensitymaxcalc,  -- Convert to lb/gal
        muddensitymincalc / 119.826428404623 as muddensitymincalc,  -- Convert to lb/gal
        mudtypcalc,
        
        -- Objectives and planning
        objective,
        objectivegeo,
        
        -- Performance percentages (converted to percentages)
        pctproblemtimecalc / 0.01 as pctproblemtimecalc,
        percenttmrotatingcalc / 0.01 as percenttmrotatingcalc,
        percenttmslidingcalc / 0.01 as percenttmslidingcalc,
        percentdepthrotatingcalc / 0.01 as percentdepthrotatingcalc,
        percentdepthslidingcalc / 0.01 as percentdepthslidingcalc,
        
        -- Program mud density
        programmuddensitymaxcalc / 119.826428404623 as programmuddensitymaxcalc,  -- Convert to lb/gal
        programmuddensitymincalc / 119.826428404623 as programmuddensitymincalc,  -- Convert to lb/gal
        
        projectrefnumbercalc,
        
        -- Production rates (converted to field units)
        ratetargetcond / 0.1589873 as ratetargetcond,  -- Convert to bbl/day
        rateactualcond / 0.1589873 as rateactualcond,  -- Convert to bbl/day
        ratetargetgas / 28.316846592 as ratetargetgas,  -- Convert to MCF/day
        rateactualgas / 28.316846592 as rateactualgas,  -- Convert to MCF/day
        ratetargetoil / 0.1589873 as ratetargetoil,  -- Convert to bbl/day
        rateactualoil / 0.1589873 as rateactualoil,  -- Convert to bbl/day
        ratetargetwater / 0.1589873 as ratetargetwater,  -- Convert to bbl/day
        rateactualwater / 0.1589873 as rateactualwater,  -- Convert to bbl/day
        
        -- Ratio calculations (converted to percentages)
        ratiodepthactualplancalc / 0.01 as ratiodepthactualplancalc,
        ratiodepthactualtargetcalc / 0.01 as ratiodepthactualtargetcalc,
        
        ratiodurtimelogrefhourscalc,
        reportnocalc,
        
        -- Responsible groups
        responsiblegrp1,
        responsiblegrp2,
        responsiblegrp3,
        
        resulttechnical,
        
        -- Rate of Penetration (ROP) calculations (converted to ft/hr)
        ropavgfromspudcalc / 7.3152 as ropavgfromspudcalc,
        ropcalc / 7.3152 as ropcalc,
        roprotatingcalc / 7.3152 as roprotatingcalc,
        ropslidingcalc / 7.3152 as ropslidingcalc,
        ropspudtimelogcalc / 7.3152 as ropspudtimelogcalc,
        roptimelogcalc / 7.3152 as roptimelogcalc,
        
        -- Safety incidents
        safetyincnocalc,
        safetyincreportnocalc,
        
        -- Status and summary
        status1,
        status2,
        summary,
        summarygeo,
        
        -- Target measurements (converted to feet)
        targetdepth / 0.3048 as targetdepth,
        targetdepthtvdcalc / 0.3048 as targetdepthtvdcalc,
        targetform,
        
        -- Time calculations (converted to hours)
        tmcirccalc / 0.0416666666666667 as tmcirccalc,
        tmdrillcalc / 0.0416666666666667 as tmdrillcalc,
        tmothercalc / 0.0416666666666667 as tmothercalc,
        tmrotatingcalc / 0.0416666666666667 as tmrotatingcalc,
        tmslidingcalc / 0.0416666666666667 as tmslidingcalc,
        tmtripcalc / 0.0416666666666667 as tmtripcalc,
        
        -- Total depth measurements (converted to feet)
        totaldepthcalc / 0.3048 as totaldepthcalc,
        totaldepthtvdcalc / 0.3048 as totaldepthtvdcalc,
        tdtomudcalc / 0.3048 as tdtomudcalc,
        
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
        
        -- Variance calculations
        varianceafefinalcalc,
        variancefieldcalc,
        variancefieldfinalcalc,
        variancefinalcalc,
        variancenormafefinalcalc,
        variancenormfieldcalc,
        variancenormfieldfinalcalc,
        variancenormfinalcalc,
        
        wvtyp,
        
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