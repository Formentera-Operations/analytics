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
        idrec as job_id,
        idwell as well_id,
        idrecwellbore as wellbore_id,
        idrecwellboretk as wellbore_table_key,
        
        -- Job classification
        wvtyp as wellview_job_category,
        jobtyp as primary_job_type,
        jobsubtyp as secondary_job_type,
        complexityindex as complexity_index,
        
        -- Key dates
        dttmstart as job_start_datetime,
        dttmend as job_end_datetime,
        dttmspud as spud_datetime,
        dttmstartplan as planned_start_datetime,
        dttmendcalc as calculated_end_datetime,
        dttmtotaldepthcalc as total_depth_reached_datetime,
        
        -- Planned dates
        dttmendplanmlcalc as planned_end_ml_datetime,
        dttmendplanmincalc as planned_end_min_datetime,
        dttmendplanmaxcalc as planned_end_max_datetime,
        dttmendplantechlimitcalc as planned_end_tech_limit_datetime,
        
        -- Objectives and targets
        objective as job_objective,
        objectivegeo as geological_objective,
        targetform as target_formation,
        
        -- Depths (converted to US units)
        targetdepth / 0.3048 as target_depth_ft,
        targetdepthtvdcalc / 0.3048 as target_depth_tvd_ft,
        totaldepthcalc / 0.3048 as total_depth_reached_ft,
        totaldepthtvdcalc / 0.3048 as total_depth_tvd_reached_ft,
        depthdrilledcalc / 0.3048 as depth_drilled_ft,
        depthrotatingcalc / 0.3048 as depth_rotating_ft,
        depthslidingcalc / 0.3048 as depth_sliding_ft,
        depthplanmaxcalc / 0.3048 as planned_max_depth_ft,
        depthdrilledperbhacalc / 0.3048 as depth_drilled_per_bha_ft,
        depthdrilledperreportnocalc / 0.3048 as depth_drilled_per_report_ft,
        depthperratiodurationcalc / 0.3048 as depth_per_ratio_duration_ft,
        tdtomudcalc / 0.3048 as td_to_mud_line_ft,
        
        -- Drilling rates (converted to US units - ft/hr)
        ropcalc / 7.3152 as rop_ft_per_hr,
        ropavgfromspudcalc / 7.3152 as rop_avg_from_spud_ft_per_hr,
        roprotatingcalc / 7.3152 as rop_rotating_ft_per_hr,
        ropslidingcalc / 7.3152 as rop_sliding_ft_per_hr,
        ropspudtimelogcalc / 7.3152 as rop_spud_time_log_ft_per_hr,
        roptimelogcalc / 7.3152 as rop_time_log_ft_per_hr,
        depthdrilledspudtorrcalc / 7.3152 as depth_drilled_spud_to_rr_ft_per_hr,
        depthperdurplanmlcalc / 7.3152 as planned_ml_rate_ft_per_hr,
        
        -- Time durations (converted to appropriate US units)
        -- Days remain as days
        durationiltcalc as duration_ilt_days,
        durationmaxtotalcalc as duration_max_total_days,
        durationmintotalcalc as duration_min_total_days,
        durationmltotalcalc as duration_ml_total_days,
        durstarttoendcalc as duration_start_to_end_days,
        durationtechlimittotalcalc as duration_tech_limit_total_days,
        durmltotalnoplanchangecalc as duration_ml_no_plan_change_days,
        durmlnoexcludecalc as duration_ml_no_exclude_days,
        
        -- Convert days to hours for operational activities
        tmdrillcalc / 0.0416666666666667 as drilling_time_hours,
        tmrotatingcalc / 0.0416666666666667 as rotating_time_hours,
        tmslidingcalc / 0.0416666666666667 as sliding_time_hours,
        tmcirccalc / 0.0416666666666667 as circulating_time_hours,
        tmtripcalc / 0.0416666666666667 as tripping_time_hours,
        tmothercalc / 0.0416666666666667 as other_time_hours,
        durationtimelogtotalcalc / 0.0416666666666667 as time_log_total_hours,
        durationspudtimelogcalc / 0.0416666666666667 as spud_time_log_hours,
        durationspudtotdcalc / 0.0416666666666667 as spud_to_td_hours,
        durationspudtorrcalc / 0.0416666666666667 as spud_to_rr_hours,
        durationnoproblemtimecalc / 0.0416666666666667 as no_problem_time_hours,
        durationproblemtimecalc / 0.0416666666666667 as problem_time_hours,
        durationpersonnelregcalc / 0.0416666666666667 as personnel_regular_hours,
        durationpersonnelotcalc / 0.0416666666666667 as personnel_ot_hours,
        durationpersonneltotcalc / 0.0416666666666667 as personnel_total_hours,
        durationspudtoplanmlcalc / 0.0416666666666667 as spud_to_plan_ml_hours,
        durationspudtoplanmincalc / 0.0416666666666667 as spud_to_plan_min_hours,
        durationspudtoplanmaxcalc / 0.0416666666666667 as spud_to_plan_max_hours,
        durationspudtoplantechlimcalc / 0.0416666666666667 as spud_to_plan_tech_limit_hours,
        estproblemtimecalc / 0.0416666666666667 as estimated_problem_time_hours,
        esttimesavecalc / 0.0416666666666667 as estimated_time_savings_hours,
        
        -- Convert days to minutes for short duration activities
        duroffbtmcalc / 0.000694444444444444 as duration_off_bottom_minutes,
        duronbtmcalc / 0.000694444444444444 as duration_on_bottom_minutes,
        durpipemovingcalc / 0.000694444444444444 as duration_pipe_moving_minutes,
        
        -- AFE and cost information (no unit conversion needed)
        afenumbercalc as afe_number,
        afenumbersuppcalc as afe_supplemental_number,
        afeamtcalc as afe_amount,
        afeamtnormcalc as afe_amount_normalized,
        afesupamtcalc as afe_supplemental_amount,
        afesupamtnormcalc as afe_supplemental_amount_normalized,
        afetotalcalc as afe_total_amount,
        afetotalnormcalc as afe_total_amount_normalized,
        afecosttypcalc as afe_cost_type,
        
        -- Cost metrics (converted to cost per hour and cost per foot)
        afeperdurmlcalc / 24 as afe_per_hour,
        afeperdurmlnormcalc / 24 as afe_per_hour_normalized,
        costpertldurcalc / 24 as cost_per_hour,
        costpertldurnormcalc / 24 as cost_per_hour_normalized,
        
        afepertargetdepthcalc / 3.28083989501312 as afe_per_target_depth_per_ft,
        afepertargetdepthnormcalc / 3.28083989501312 as afe_per_target_depth_normalized_per_ft,
        costperdepthcalc / 3.28083989501312 as cost_per_depth_per_ft,
        costnormperdepthcalc / 3.28083989501312 as cost_normalized_per_depth_per_ft,
        costperdepthplanmlcalc / 3.28083989501312 as cost_per_depth_plan_ml_per_ft,
        costnormperdepthplanmlcalc / 3.28083989501312 as cost_normalized_per_depth_plan_ml_per_ft,
        costperlateralcalc / 3.28083989501312 as cost_per_lateral_per_ft,
        costperlateralallcalc / 3.28083989501312 as cost_per_lateral_all_per_ft,
        costnormperlateralcalc / 3.28083989501312 as cost_normalized_per_lateral_per_ft,
        costnormperlateralallcalc / 3.28083989501312 as cost_normalized_per_lateral_all_per_ft,
        mudcostperdepthcalc / 3.28083989501312 as mud_cost_per_depth_per_ft,
        mudcostperdepthnormcalc / 3.28083989501312 as mud_cost_per_depth_normalized_per_ft,
        
        -- Other cost fields (no conversion)
        costfinalactual as final_actual_cost,
        costtotalcalc as total_cost,
        costmaxtotalcalc as max_total_cost,
        costmintotalcalc as min_total_cost,
        costmltotalcalc as ml_total_cost,
        costmltotalnoplanchangecalc as ml_total_cost_no_plan_change,
        costmlnoexcludecalc as ml_cost_no_exclude,
        costtechlimittotalcalc as tech_limit_total_cost,
        costnormtotalcalc as total_cost_normalized,
        costforecastcalc as forecast_cost,
        costnormforecastcalc as forecast_cost_normalized,
        
        -- Cost variances (no conversion)
        costafeforecastvarcalc as cost_afe_forecast_variance,
        costforecastfieldvarcalc as cost_forecast_field_variance,
        costnormafeforecastvarcalc as cost_normalized_afe_forecast_variance,
        costnormforecastfieldvarcalc as cost_normalized_forecast_field_variance,
        varianceafefinalcalc as variance_afe_final,
        variancefieldcalc as variance_field,
        variancefieldfinalcalc as variance_field_final,
        variancefinalcalc as variance_final,
        variancenormafefinalcalc as variance_normalized_afe_final,
        variancenormfieldcalc as variance_normalized_field,
        variancenormfieldfinalcalc as variance_normalized_field_final,
        variancenormfinalcalc as variance_normalized_final,
        estcostsavecalc as estimated_cost_savings,
        estcostnormsavecalc as estimated_cost_savings_normalized,
        estproblemcostcalc as estimated_problem_cost,
        estproblemcostnormcalc as estimated_problem_cost_normalized,
        
        -- Mud and supply costs (no conversion)
        mudcostcalc as mud_cost,
        mudcostnormcalc as mud_cost_normalized,
        jobsupplycostcalc as job_supply_cost,
        jobsupplycostnormcalc as job_supply_cost_normalized,
        finalinvoicetotalcalc as final_invoice_total,
        finalinvoicetotalnormcalc as final_invoice_total_normalized,
        
        -- Currency information
        currencycode as currency_code,
        currencyexchangerate as currency_exchange_rate,
        
        -- Mud properties (converted to US units)
        muddensitymaxcalc / 119.826428404623 as mud_density_max_ppg,
        muddensitymincalc / 119.826428404623 as mud_density_min_ppg,
        programmuddensitymaxcalc / 119.826428404623 as program_mud_density_max_ppg,
        programmuddensitymincalc / 119.826428404623 as program_mud_density_min_ppg,
        mudtypcalc as mud_type,
        
        -- Production rates (converted to US units)
        ratetargetoil / 0.1589873 as target_oil_rate_bbl_per_day,
        rateactualoil / 0.1589873 as actual_oil_rate_bbl_per_day,
        ratetargetwater / 0.1589873 as target_water_rate_bbl_per_day,
        rateactualwater / 0.1589873 as actual_water_rate_bbl_per_day,
        ratetargetcond / 0.1589873 as target_condensate_rate_bbl_per_day,
        rateactualcond / 0.1589873 as actual_condensate_rate_bbl_per_day,
        ratetargetgas / 28.316846592 as target_gas_rate_mcf_per_day,
        rateactualgas / 28.316846592 as actual_gas_rate_mcf_per_day,
        
        -- Percentages (converted from proportions to percentages)
        pctproblemtimecalc / 0.01 as problem_time_percentage,
        percenttmrotatingcalc / 0.01 as rotating_time_percentage,
        percenttmslidingcalc / 0.01 as sliding_time_percentage,
        percentdepthrotatingcalc / 0.01 as depth_rotating_percentage,
        percentdepthslidingcalc / 0.01 as depth_sliding_percentage,
        ratiodepthactualplancalc / 0.01 as ratio_depth_actual_plan_percentage,
        ratiodepthactualtargetcalc / 0.01 as ratio_depth_actual_target_percentage,
        
        -- Performance metrics (no conversion)
        bhadrillruncalc as bha_drill_runs,
        bhatotalruncalc as bha_total_runs,
        bitrevscalc as bit_revolutions,
        reportnocalc as report_count,
        ratiodurtimelogrefhourscalc as ratio_duration_time_log_ref_hours,
        
        -- Safety metrics
        safetyincnocalc as safety_incident_count,
        safetyincreportnocalc as safety_reportable_incident_count,
        hazardidnorptcalc as hazard_id_report_count,
        
        -- Status and results
        status1 as primary_status,
        status2 as secondary_status,
        resulttechnical as technical_result,
        summary as job_summary,
        summarygeo as geological_summary,
        
        -- Responsible parties
        client as client_operator,
        responsiblegrp1 as responsible_group_1,
        responsiblegrp2 as responsible_group_2,
        responsiblegrp3 as responsible_group_3,
        
        -- Reference identifiers
        projectrefnumbercalc as project_reference_number,
        chartofaccounts as chart_of_accounts,
        jobida as job_id_a,
        jobidb as job_id_b,
        jobidc as job_id_c,
        idreclastrigcalc as last_rig_id,
        idreclastrigcalctk as last_rig_table_key,
        idrectub as tubing_string_id,
        idrectubtk as tubing_string_table_key,
        idrecwellborecalc as calculated_wellbore_id,
        idrecwellborecalctk as calculated_wellbore_table_key,
        
        -- User fields
        usertxt1 as user_text_1,
        usertxt2 as user_text_2,
        usertxt3 as user_text_3,
        usertxt4 as user_text_4,
        usertxt5 as user_text_5,
        usernum1 as user_number_1,
        usernum2 as user_number_2,
        usernum3 as user_number_3,
        usernum4 as user_number_4,
        usernum5 as user_number_5,
        case when userboolean1 = 1 then true else false end as user_boolean_1,
        case when userboolean2 = 1 then true else false end as user_boolean_2,
        
        -- System fields
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,
        syslockdate as system_lock_date,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        
        -- Fivetran fields
        _fivetran_synced as fivetran_synced_at
        
    from source_data
)

select * from renamed