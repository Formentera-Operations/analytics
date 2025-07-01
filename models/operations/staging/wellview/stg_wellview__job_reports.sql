{{ config(
    materialized='view',
    tags=['wellview', 'job-reports', 'daily-reports', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBREPORT') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as report_id,
        idrecparent as job_id,
        idwell as well_id,
        
        -- Report period information
        dttmstart as report_start_datetime,
        dttmend as report_end_datetime,
        reportnocalc as report_number,
        reportdaycalc as report_day,
        daysfromspudcalc as days_from_spud,
        daysfromspudtorrcalc as days_from_spud_to_rr,
        
        -- Depths (converted to US units)
        depthenddpcalc / 0.3048 as end_depth_ft,
        depthstartdpcalc / 0.3048 as start_depth_ft,
        depthprogressdpcalc / 0.3048 as depth_progress_ft,
        depthnetprogressdpcalc / 0.3048 as net_depth_progress_ft,
        depthrotatingcalc / 0.3048 as depth_rotating_ft,
        depthslidingcalc / 0.3048 as depth_sliding_ft,
        depthtvdenddpcalc / 0.3048 as end_depth_tvd_ft,
        depthtvdstartdpcalc / 0.3048 as start_depth_tvd_ft,
        depthenddpcumcalc / 0.3048 as cumulative_end_depth_ft,
        depthenddpnullcalc / 0.3048 as end_depth_null_ft,
        depthstartdpnullcalc / 0.3048 as start_depth_null_ft,
        
        -- Drilling rates (converted to US units - ft/hr)
        ropcalc / 7.3152 as rop_ft_per_hr,
        roprotatingcalc / 7.3152 as rop_rotating_ft_per_hr,
        ropslidingcalc / 7.3152 as rop_sliding_ft_per_hr,
        depthperdurcalc / 7.3152 as depth_per_duration_ft_per_hr,
        depthperdurvarcalc / 7.3152 as depth_per_duration_variance_ft_per_hr,
        
        -- Time durations (converted to hours)
        tmdrillcalc / 0.0416666666666667 as drilling_time_hours,
        tmrotatingcalc / 0.0416666666666667 as rotating_time_hours,
        tmslidingcalc / 0.0416666666666667 as sliding_time_hours,
        tmcirccalc / 0.0416666666666667 as circulating_time_hours,
        tmtripcalc / 0.0416666666666667 as tripping_time_hours,
        tmothercalc / 0.0416666666666667 as other_time_hours,
        durationtimelogtotalcalc / 0.0416666666666667 as total_time_log_hours,
        durationnoprobtimecalc / 0.0416666666666667 as no_problem_time_hours,
        durationproblemtimecalc / 0.0416666666666667 as problem_time_hours,
        rigtime / 0.0416666666666667 as rig_time_hours,
        
        -- Cumulative time durations (converted to hours)
        tmdrillcumcalc / 0.0416666666666667 as cumulative_drilling_time_hours,
        tmcirccumcalc / 0.0416666666666667 as cumulative_circulating_time_hours,
        tmtripcumcalc / 0.0416666666666667 as cumulative_tripping_time_hours,
        tmothercumcalc / 0.0416666666666667 as cumulative_other_time_hours,
        tmcirctripothercalc / 0.0416666666666667 as circ_trip_other_time_hours,
        tmcirctripothercumcalc / 0.0416666666666667 as cumulative_circ_trip_other_hours,
        tmdrillnoexccalc / 0.0416666666666667 as drilling_time_no_exclude_hours,
        tmdrillcumnoexccalc / 0.0416666666666667 as cumulative_drilling_no_exclude_hours,
        durationnoprobtimecumcalc / 0.0416666666666667 as cumulative_no_problem_time_hours,
        durationproblemtimecumcalc / 0.0416666666666667 as cumulative_problem_time_hours,
        rigtimecumcalc / 0.0416666666666667 as cumulative_rig_time_hours,
        
        -- Personnel hours (converted to hours)
        durationpersonnelregcalc / 0.0416666666666667 as personnel_regular_hours,
        durationpersonnelotcalc / 0.0416666666666667 as personnel_overtime_hours,
        durationpersonneltotcalc / 0.0416666666666667 as personnel_total_hours,
        durpersonnelregcumcalc / 0.0416666666666667 as cumulative_personnel_regular_hours,
        durpersonnelotcumcalc / 0.0416666666666667 as cumulative_personnel_overtime_hours,
        durpersonneltotcumcalc / 0.0416666666666667 as cumulative_personnel_total_hours,
        
        -- Duration in days (remain as days)
        rigdayscalc as rig_days,
        rigdayscumcalc as cumulative_rig_days,
        durstarttoendcalc as duration_start_to_end_days,
        durprojectedmljobcalc as projected_ml_job_duration_days,
        durprojectedminjobcalc as projected_min_job_duration_days,
        durprojectedmaxjobcalc as projected_max_job_duration_days,
        durprojectedtljobcalc as projected_tl_job_duration_days,
        durprojectedmlphasecalc as projected_ml_phase_duration_days,
        durprojectedminphasecalc as projected_min_phase_duration_days,
        durprojectedmaxphasecalc as projected_max_phase_duration_days,
        durprojectedtlphasecalc as projected_tl_phase_duration_days,
        durationtimelogcum12hrcalc as time_log_cumulative_12hr_days,
        durationtimelogcumspudcalc as time_log_cumulative_spud_days,
        durationtimelogcumspudrrcalc as time_log_cumulative_spud_rr_days,
        durationtimelogtotcumcalc as total_time_log_cumulative_days,
        durlastsinccalc as duration_last_since_days,
        durlastsincreportcalc as duration_last_since_report_days,
        durlastsincrptdaycalc as duration_last_since_report_day_days,
        durlastsincreportrptdaycalc as duration_last_since_report_report_day_days,
        durnoprobtimecumdayscalc as cumulative_no_problem_time_days,
        durproblemtimecumdayscalc as cumulative_problem_time_days,
        durationsinceltinc as duration_since_lti_days,
        durationsincerptinc as duration_since_reportable_incident_days,
        
        -- Time ahead/behind schedule (days)
        timeaheadmljobcalc as time_ahead_ml_job_days,
        timeaheadminjobcalc as time_ahead_min_job_days,
        timeaheadmaxjobcalc as time_ahead_max_job_days,
        timeaheadtljobcalc as time_ahead_tl_job_days,
        timeaheadmlphasecalc as time_ahead_ml_phase_days,
        timeaheadminphasecalc as time_ahead_min_phase_days,
        timeaheadmaxphasecalc as time_ahead_max_phase_days,
        timeaheadtlphasecalc as time_ahead_tl_phase_days,
        
        -- Short duration activities (converted to minutes)
        duroffbtmcalc / 0.000694444444444444 as duration_off_bottom_minutes,
        duronbtmcalc / 0.000694444444444444 as duration_on_bottom_minutes,
        durpipemovingcalc / 0.000694444444444444 as duration_pipe_moving_minutes,
        
        -- BHA and bit performance
        bhatotalruncalc as bha_total_runs,
        bitrevscalc as bit_revolutions,
        
        -- Cost information (converted to US units)
        costperdepthcalc / 3.28083989501312 as cost_per_depth_per_ft,
        costperdepthcumcalc / 3.28083989501312 as cumulative_cost_per_depth_per_ft,
        costperdepthvarcalc / 3.28083989501312 as cost_per_depth_variance_per_ft,
        costperdepthnormcalc / 3.28083989501312 as cost_per_depth_normalized_per_ft,
        costpertldurcalc / 24 as cost_per_hour,
        costpertldurnormcalc / 24 as cost_per_hour_normalized,
        mudcostperdepthcalc / 3.28083989501312 as mud_cost_per_depth_per_ft,
        mudcostperdepthcumcalc / 3.28083989501312 as cumulative_mud_cost_per_depth_per_ft,
        mudcostperdepthnormcalc / 3.28083989501312 as mud_cost_per_depth_normalized_per_ft,
        mudcostperdepthcumnormcalc / 3.28083989501312 as cumulative_mud_cost_per_depth_normalized_per_ft,
        
        -- Total costs (no conversion)
        costtodatecalc as cost_to_date,
        costtodatenormcalc as cost_to_date_normalized,
        costtotalcalc as total_cost,
        costtotalnormcalc as total_cost_normalized,
        costjobsupplyamtcalc as job_supply_cost,
        costjobsupplyamtnormcalc as job_supply_cost_normalized,
        costjobsupplyamttodatecalc as job_supply_cost_to_date,
        costjobsupplyamttodtncalc as job_supply_cost_to_date_normalized,
        costmudaddcalc as mud_additive_cost,
        costmudaddnormcalc as mud_additive_cost_normalized,
        costmudaddtodatecalc as mud_additive_cost_to_date,
        costmudaddtodatenormcalc as mud_additive_cost_to_date_normalized,
        
        -- Projected costs (no conversion)
        costprojectedmljobcalc as projected_ml_job_cost,
        costprojectedminjobcalc as projected_min_job_cost,
        costprojectedmaxjobcalc as projected_max_job_cost,
        costprojectedtljobcalc as projected_tl_job_cost,
        costprojectedmljobnormcalc as projected_ml_job_cost_normalized,
        costprojectedminjobnormcalc as projected_min_job_cost_normalized,
        costprojectedmaxjobnormcalc as projected_max_job_cost_normalized,
        costprojectedtljobnormcalc as projected_tl_job_cost_normalized,
        costprojectedmlphasecalc as projected_ml_phase_cost,
        costprojectedminphasecalc as projected_min_phase_cost,
        costprojectedmaxphasecalc as projected_max_phase_cost,
        costprojectedtlphasecalc as projected_tl_phase_cost,
        costprojectedmlphasenormcalc as projected_ml_phase_cost_normalized,
        costprojectedminphasenormcalc as projected_min_phase_cost_normalized,
        costprojectedmaxphasenormcalc as projected_max_phase_cost_normalized,
        costprojectedtlphasenormcalc as projected_tl_phase_cost_normalized,
        
        -- Cost variances (no conversion)
        costforecastfieldvarcalc as cost_forecast_field_variance,
        costnormforecastfieldvarcalc as cost_normalized_forecast_field_variance,
        
        -- Mud properties (converted to US units)
        lastmuddensitycalc / 119.826428404623 as last_mud_density_ppg,
        
        -- Volumes (converted to US units)
        volholecalc / 0.158987294928 as hole_volume_bbl,
        volmudactivecalc / 0.158987294928 as active_mud_volume_bbl,
        volmudactivevarcalc / 0.158987294928 as active_mud_volume_variance_bbl,
        volmudbalancecalc / 0.158987294928 as mud_balance_volume_bbl,
        volholevarcalc / 0.158987294928 as hole_volume_variance_bbl,
        volmudaddedcalc / 0.158987294928 as mud_volume_added_bbl,
        volmudaddedcumcalc / 0.158987294928 as cumulative_mud_volume_added_bbl,
        volmudlosscalc / 0.158987294928 as mud_volume_lost_bbl,
        volmudlosscumcalc / 0.158987294928 as cumulative_mud_volume_lost_bbl,
        volmudaddedlossvarcalc / 0.158987294928 as mud_added_loss_variance_bbl,
        volmudaddedlossvarcumcalc / 0.158987294928 as cumulative_mud_added_loss_variance_bbl,
        volmudtankcalc / 0.158987294928 as mud_tank_volume_bbl,
        volbittoshoecalc / 0.158987294928 as bit_to_shoe_volume_bbl,
        volcastoptorisertopcalc / 0.158987294928 as casing_top_to_riser_top_volume_bbl,
        volpumptobitcalc / 0.158987294928 as pump_to_bit_volume_bbl,
        volshoetocastopcalc / 0.158987294928 as shoe_to_casing_top_volume_bbl,
        
        -- Gas readings (converted to percentages)
        gasbackgroundavg / 0.01 as background_gas_avg_percent,
        gasbackgroundmax / 0.01 as background_gas_max_percent,
        gasdrillavg / 0.01 as drilling_gas_avg_percent,
        gasdrillmax / 0.01 as drilling_gas_max_percent,
        gasconnectionavg / 0.01 as connection_gas_avg_percent,
        gasconnectionmax / 0.01 as connection_gas_max_percent,
        gastripavg / 0.01 as trip_gas_avg_percent,
        gastripmax / 0.01 as trip_gas_max_percent,
        
        -- H2S (converted to ppm)
        h2smax / 1E-06 as h2s_max_ppm,
        
        -- Percentages (converted from proportions)
        pctproblemtimecalc / 0.01 as problem_time_percentage,
        pctproblemtimecumcalc / 0.01 as cumulative_problem_time_percentage,
        percentfieldafecalc / 0.01 as percent_field_afe,
        percentcompletemljobcalc / 0.01 as percent_complete_ml_job,
        percentcompleteminjobcalc / 0.01 as percent_complete_min_job,
        percentcompletemaxjobcalc / 0.01 as percent_complete_max_job,
        percentcompletetljobcalc / 0.01 as percent_complete_tl_job,
        percentcompletemlphasecalc / 0.01 as percent_complete_ml_phase,
        percentcompleteminphasecalc / 0.01 as percent_complete_min_phase,
        percentcompletemaxphasecalc / 0.01 as percent_complete_max_phase,
        percentcompletetlphasecalc / 0.01 as percent_complete_tl_phase,
        percentdepthrotatingcalc / 0.01 as percent_depth_rotating,
        percentdepthslidingcalc / 0.01 as percent_depth_sliding,
        percenttmrotatingcalc / 0.01 as percent_time_rotating,
        percenttmslidingcalc / 0.01 as percent_time_sliding,
        ratiodurprojmlplancalc / 0.01 as ratio_duration_projected_ml_plan_percent,
        ratiodurprojminplancalc / 0.01 as ratio_duration_projected_min_plan_percent,
        ratiodurprojmaxplancalc / 0.01 as ratio_duration_projected_max_plan_percent,
        ratiodurprojtlplancalc / 0.01 as ratio_duration_projected_tl_plan_percent,
        
        -- Safety metrics
        safetyincnocalc as safety_incident_count,
        safetyinccalc as safety_incident_calc,
        safetyincnocumcalc as cumulative_safety_incident_count,
        safetyincratecalc as safety_incident_rate,
        safetyincreportcalc as safety_incident_report_calc,
        safetyincreportnocalc as reportable_safety_incident_count,
        safetyincreportnocumcalc as cumulative_reportable_safety_incident_count,
        safetyincreportratecalc as reportable_safety_incident_rate,
        hazardidnorptcalc as hazard_id_report_count,
        hazardidnorptcumcalc as cumulative_hazard_id_report_count,
        
        -- Personnel
        headcountcalc as head_count,
        
        -- Environmental conditions
        condtemp / 0.555555555555556 + 32 as temperature_fahrenheit,
        condhole as hole_conditions,
        condlease as lease_conditions,
        condroad as road_conditions,
        condwave as wave_conditions,
        condweather as weather_conditions,
        condwind as wind_conditions,
        
        -- Status and operational info
        statusend as report_status_end,
        summaryops as operations_summary,
        remarks as remarks,
        contactcalc as contact_calc,
        
        -- Planning and next operations
        plannextrptops as planned_next_report_ops,
        rpttmactops as report_time_actual_ops,
        
        -- Time log codes
        timelogcode1calc as time_log_code_1,
        timelogcode2calc as time_log_code_2,
        timelogcode3calc as time_log_code_3,
        timelogcode4calc as time_log_code_4,
        
        -- Projected end dates
        dttmprojendmljobcalc as projected_end_date_ml_job,
        dttmprojendminjobcalc as projected_end_date_min_job,
        dttmprojendmaxjobcalc as projected_end_date_max_job,
        dttmprojendtljobcalc as projected_end_date_tl_job,
        dttmprojendmlphasecalc as projected_end_date_ml_phase,
        dttmprojendminphasecalc as projected_end_date_min_phase,
        dttmprojendmaxphasecalc as projected_end_date_max_phase,
        dttmprojendtlphasecalc as projected_end_date_tl_phase,
        
        -- Depth projection method
        depthtvdendprojmethod as depth_tvd_end_projection_method,
        
        -- Lesson and problem indicators
        intlessoncalc as lessons_learned_indicator,
        intproblemcalc as problems_indicator,
        
        -- Rig information
        rigscalc as rigs_calc,
        
        -- Weight calculations (converted to US units - lbs)
        weightmetalrecovtotalcalc / 0.45359237 as total_metal_recovery_weight_lbs,
        
        -- Foreign keys and relationships
        idrecwellborecalc as wellbore_id,
        idrecwellborecalctk as wellbore_table_key,
        idrecjobprogramphasecalc as job_program_phase_id,
        idrecjobprogramphasecalctk as job_program_phase_table_key,
        idreclastcascalc as last_casing_id,
        idreclastcascalctk as last_casing_table_key,
        idrecnextcas as next_casing_id,
        idrecnextcastk as next_casing_table_key,
        idreclastrigcalc as last_rig_id,
        idreclastrigcalctk as last_rig_table_key,
        
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
        userboolean1 as user_boolean_1,
        userboolean2 as user_boolean_2,
        
        -- System fields
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockdate as system_lock_date,
        
        -- Fivetran metadata
        _fivetran_synced as fivetran_synced_at

    from source_data
)

select * from renamed