{{ config(
    materialized='view',
    tags=['wellview', 'jobs', 'operations', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOB') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as "Job ID",
        idwell as "Well ID",
        idrecwellbore as "Wellbore ID",
        idrecwellboretk as "Wellbore Table Key",

        -- Job classification
        wvtyp as "Wellview Job Category",
        jobtyp as "Primary Job Type",
        jobsubtyp as "Secondary Job Type",
        complexityindex as "Complexity Index",

        -- Key dates
        dttmstart as "Job Start Datetime",
        dttmend as "Job End Datetime",
        dttmspud as "Spud Datetime",
        dttmstartplan as "Planned Start Datetime",
        dttmendcalc as "Calculated End Datetime",
        dttmtotaldepthcalc as "Total Depth Reached Datetime",

        -- Planned dates
        dttmendplanmlcalc as "Planned End Ml Datetime",
        dttmendplanmincalc as "Planned End min Datetime",
        dttmendplanmaxcalc as "Planned End Max Datetime",
        dttmendplantechlimitcalc as "Planned End Tech Limit Datetime",

        -- Objectives and targets
        objective as "Job Objective",
        objectivegeo as "Geological Objective",
        targetform as "Target Formation",

        -- Depths (converted to US units)
        targetdepth / 0.3048 as "Target Depth Ft",
        targetdepthtvdcalc / 0.3048 as "Target Depth Tvd Ft",
        totaldepthcalc / 0.3048 as "Total Depth Reached Ft",
        totaldepthtvdcalc / 0.3048 as "Total Depth Tvd Reached Ft",
        depthdrilledcalc / 0.3048 as "Depth Drilled Ft",
        depthrotatingcalc / 0.3048 as "Depth Rotating Ft",
        depthslidingcalc / 0.3048 as "Depth Sliding Ft",
        depthplanmaxcalc / 0.3048 as "Planned Max Depth Ft",
        depthdrilledperbhacalc / 0.3048 as "Depth Drilled per Bha Ft",
        depthdrilledperreportnocalc / 0.3048 as "Depth Drilled per Report Ft",
        depthperratiodurationcalc / 0.3048 as "Depth per Ratio Duration Ft",
        tdtomudcalc / 0.3048 as "Td To Mud Line Ft",

        -- Drilling rates (converted to US units - ft/hr)
        ropcalc / 7.3152 as "Rop Ft Per hr",
        ropavgfromspudcalc / 7.3152 as "Rop Avg From Spud Ft Per hr",
        roprotatingcalc / 7.3152 as "Rop Rotating Ft Per hr",
        ropslidingcalc / 7.3152 as "Rop Sliding Ft Per hr",
        ropspudtimelogcalc / 7.3152 as "Rop Spud Time Log Ft Per hr",
        roptimelogcalc / 7.3152 as "Rop Time Log Ft Per hr",
        depthdrilledspudtorrcalc / 7.3152 as "Depth Drilled Spud To Rr Ft Per hr",
        depthperdurplanmlcalc / 7.3152 as "Planned Ml Rate Ft Per hr",

        -- Time durations (converted to appropriate US units)
        -- Days remain as days
        durationiltcalc as "Duration Ilt Days",
        durationmaxtotalcalc as "Duration Max Total Days",
        durationmintotalcalc as "Duration min Total Days",
        durationmltotalcalc as "Duration Ml Total Days",
        durstarttoendcalc as "Duration Start To End Days",
        durationtechlimittotalcalc as "Duration Tech Limit Total Days",
        durmltotalnoplanchangecalc as "Duration Ml No Plan Change Days",
        durmlnoexcludecalc as "Duration Ml No Exclude Days",

        -- Convert days to hours for operational activities
        tmdrillcalc / 0.0416666666666667 as "Drilling Time Hours",
        tmrotatingcalc / 0.0416666666666667 as "Rotating Time Hours",
        tmslidingcalc / 0.0416666666666667 as "Sliding Time Hours",
        tmcirccalc / 0.0416666666666667 as "Circulating Time Hours",
        tmtripcalc / 0.0416666666666667 as "Tripping Time Hours",
        tmothercalc / 0.0416666666666667 as "Other Time Hours",
        durationtimelogtotalcalc / 0.0416666666666667 as "Time Log Total Hours",
        durationspudtimelogcalc / 0.0416666666666667 as "Spud Time Log Hours",
        durationspudtotdcalc / 0.0416666666666667 as "Spud To Td Hours",
        durationspudtorrcalc / 0.0416666666666667 as "Spud To Rr Hours",
        durationnoproblemtimecalc / 0.0416666666666667 as "No Problem Time Hours",
        durationproblemtimecalc / 0.0416666666666667 as "Problem Time Hours",
        durationpersonnelregcalc / 0.0416666666666667 as "Personnel Regular Hours",
        durationpersonnelotcalc / 0.0416666666666667 as "Personnel Ot Hours",
        durationpersonneltotcalc / 0.0416666666666667 as "Personnel Total Hours",
        durationspudtoplanmlcalc / 0.0416666666666667 as "Spud To Plan Ml Hours",
        durationspudtoplanmincalc / 0.0416666666666667 as "Spud To Plan min Hours",
        durationspudtoplanmaxcalc / 0.0416666666666667 as "Spud To Plan Max Hours",
        durationspudtoplantechlimcalc / 0.0416666666666667 as "Spud To Plan Tech Limit Hours",
        estproblemtimecalc / 0.0416666666666667 as "Estimated Problem Time Hours",
        esttimesavecalc / 0.0416666666666667 as "Estimated Time Savings Hours",

        -- Convert days to minutes for short duration activities
        duroffbtmcalc / 0.000694444444444444 as "Duration Off Bottom Minutes",
        duronbtmcalc / 0.000694444444444444 as "Duration On Bottom Minutes",
        durpipemovingcalc / 0.000694444444444444 as "Duration Pipe Moving Minutes",

        -- AFE and cost information (no unit conversion needed)
        afenumbercalc as "AFE Number",
        afenumbersuppcalc as "AFE Supplemental Number",
        afeamtcalc as "AFE Amount",
        afeamtnormcalc as "AFE Amount Normalized",
        afesupamtcalc as "AFE Supplemental Amount",
        afesupamtnormcalc as "AFE Supplemental Amount Normalized",
        afetotalcalc as "AFE Total Amount",
        afetotalnormcalc as "AFE Total Amount Normalized",
        afecosttypcalc as "AFE Cost Type",

        -- Cost metrics (converted to cost per hour and cost per foot)
        afeperdurmlcalc / 24 as "AFE per Hour",
        afeperdurmlnormcalc / 24 as "AFE per Hour Normalized",
        costpertldurcalc / 24 as "Cost per Hour",
        costpertldurnormcalc / 24 as "Cost per Hour Normalized",

        afepertargetdepthcalc / 3.28083989501312 as "AFE per Target Depth per Ft",
        afepertargetdepthnormcalc / 3.28083989501312 as "AFE per Target Depth Normalized per Ft",
        costperdepthcalc / 3.28083989501312 as "Cost per Depth per Ft",
        costnormperdepthcalc / 3.28083989501312 as "Cost Normalized per Depth per Ft",
        costperdepthplanmlcalc / 3.28083989501312 as "Cost per Depth Plan Ml per Ft",
        costnormperdepthplanmlcalc / 3.28083989501312 as "Cost Normalized per Depth Plan Ml per Ft",
        costperlateralcalc / 3.28083989501312 as "Cost per Lateral per Ft",
        costperlateralallcalc / 3.28083989501312 as "Cost per Lateral All per Ft",
        costnormperlateralcalc / 3.28083989501312 as "Cost Normalized per Lateral per Ft",
        costnormperlateralallcalc / 3.28083989501312 as "Cost Normalized per Lateral All per Ft",
        mudcostperdepthcalc / 3.28083989501312 as "Mud Cost per Depth per Ft",
        mudcostperdepthnormcalc / 3.28083989501312 as "Mud Cost per Depth Normalized per Ft",

        -- Other cost fields (no conversion)
        costfinalactual as "Final Actual Cost",
        costtotalcalc as "Total Field Estimate",
        costmaxtotalcalc as "Max Total Cost",
        costmintotalcalc as "min Total Cost",
        costmltotalcalc as "Ml Total Cost",
        costmltotalnoplanchangecalc as "Ml Total Cost No Plan Change",
        costmlnoexcludecalc as "Ml Cost No Exclude",
        costtechlimittotalcalc as "Tech Limit Total Cost",
        costnormtotalcalc as "Total Cost Normalized",
        costforecastcalc as "Forecast Cost",
        costnormforecastcalc as "Forecast Cost Normalized",

        -- Cost variances (no conversion)
        costafeforecastvarcalc as "Cost AFE Forecast Variance",
        costforecastfieldvarcalc as "Cost Forecast Field Variance",
        costnormafeforecastvarcalc as "Cost Normalized AFE Forecast Variance",
        costnormforecastfieldvarcalc as "Cost Normalized Forecast Field Variance",
        varianceafefinalcalc as "Variance AFE Final",
        variancefieldcalc as "Variance Field",
        variancefieldfinalcalc as "Variance Field Final",
        variancefinalcalc as "Variance Final",
        variancenormafefinalcalc as "Variance Normalized AFE Final",
        variancenormfieldcalc as "Variance Normalized Field",
        variancenormfieldfinalcalc as "Variance Normalized Field Final",
        variancenormfinalcalc as "Variance Normalized Final",
        estcostsavecalc as "Estimated Cost Savings",
        estcostnormsavecalc as "Estimated Cost Savings Normalized",
        estproblemcostcalc as "Estimated Problem Cost",
        estproblemcostnormcalc as "Estimated Problem Cost Normalized",

        -- Mud and supply costs (no conversion)
        mudcostcalc as "Mud Cost",
        mudcostnormcalc as "Mud Cost Normalized",
        jobsupplycostcalc as "Job Supply Cost",
        jobsupplycostnormcalc as "Job Supply Cost Normalized",
        finalinvoicetotalcalc as "Final Invoice Total",
        finalinvoicetotalnormcalc as "Final Invoice Total Normalized",

        -- Currency information
        currencycode as "Currency Code",
        currencyexchangerate as "Currency Exchange Rate",

        -- Mud properties (converted to US units)
        muddensitymaxcalc / 119.826428404623 as "Mud Density Max ppg",
        muddensitymincalc / 119.826428404623 as "Mud Density Min ppg",
        programmuddensitymaxcalc / 119.826428404623 as "Program Mud Density Max ppg",
        programmuddensitymincalc / 119.826428404623 as "Program Mud Density Min ppg",
        mudtypcalc as "Mud Type",

        -- Production rates (converted to US units)
        ratetargetoil / 0.1589873 as "Target Oil Rate Bbl Per day",
        rateactualoil / 0.1589873 as "Actual Oil Rate Bbl Per day",
        ratetargetwater / 0.1589873 as "Target Water Rate Bbl Per day",
        rateactualwater / 0.1589873 as "Actual Water Rate Bbl Per day",
        ratetargetcond / 0.1589873 as "Target Condensate Rate Bbl Per day",
        rateactualcond / 0.1589873 as "Actual Condensate Rate Bbl Per day",
        ratetargetgas / 28.316846592 as "Target Gas Rate Mcf Per day",
        rateactualgas / 28.316846592 as "Actual Gas Rate Mcf Per day",

        -- Percentages (converted from proportions to percentages)
        pctproblemtimecalc / 0.01 as "Problem Time Percentage",
        percenttmrotatingcalc / 0.01 as "Rotating Time Percentage",
        percenttmslidingcalc / 0.01 as "Sliding Time Percentage",
        percentdepthrotatingcalc / 0.01 as "Depth Rotating Percentage",
        percentdepthslidingcalc / 0.01 as "Depth Sliding Percentage",
        ratiodepthactualplancalc / 0.01 as "Ratio Depth Actual Plan Percentage",
        ratiodepthactualtargetcalc / 0.01 as "Ratio Depth Actual Target Percentage",

        -- Performance metrics (no conversion)
        bhadrillruncalc as "Bha Drill Runs",
        bhatotalruncalc as "Bha Total Runs",
        bitrevscalc as "Bit Revolutions",
        reportnocalc as "Report Count",
        ratiodurtimelogrefhourscalc as "Ratio Duration Time Log Ref Hours",

        -- Safety metrics
        safetyincnocalc as "Safety Incident Count",
        safetyincreportnocalc as "Safety Reportable Incident Count",
        hazardidnorptcalc as "Hazard ID Report Count",

        -- Status and results
        status1 as "Primary Status",
        status2 as "Secondary Status",
        resulttechnical as "Technical Result",
        summary as "Job Summary",
        summarygeo as "Geological Summary",

        -- Responsible parties
        client as "Client Operator",
        responsiblegrp1 as "Responsible Group 1",
        responsiblegrp2 as "Responsible Group 2",
        responsiblegrp3 as "Responsible Group 3",

        -- Reference identifiers
        projectrefnumbercalc as "Project Reference Number",
        chartofaccounts as "Chart Of Accounts",
        jobida as "Job ID A",
        jobidb as "Job ID B",
        jobidc as "Job ID C",
        idreclastrigcalc as "Last Rig ID",
        idreclastrigcalctk as "Last Rig Table Key",
        idrectub as "Tubing String ID",
        idrectubtk as "Tubing String Table Key",
        idrecwellborecalc as "Calculated Wellbore ID",
        idrecwellborecalctk as "Calculated Wellbore Table Key",

        -- User fields
        usertxt1 as "User Text 1",
        usertxt2 as "User Text 2",
        usertxt3 as "User Text 3",
        usertxt4 as "User Text 4",
        usertxt5 as "User Text 5",
        usernum1 as "User Number 1",
        usernum2 as "User Number 2",
        usernum3 as "User Number 3",
        usernum4 as "User Number 4",
        usernum5 as "User Number 5",
        case when userboolean1 = 1 then true else false end as "User Boolean 1",
        case when userboolean2 = 1 then true else false end as "User Boolean 2",

        -- System fields
        syscreatedate as "Created At",
        syscreateuser as "Created By",
        sysmoddate as "Last Mod At",
        sysmoduser as "Last Mod By",
        systag as "System Tag",
        syslockdate as "System Lock Date",
        syslockme as "System Lock Me",
        syslockchildren as "System Lock Children",
        syslockmeui as "System Lock Me UI",
        syslockchildrenui as "System Lock Children UI",

        -- Fivetran fields
        _fivetran_synced as "Fivetran Synced At"
        
    from source_data
)

select * from renamed