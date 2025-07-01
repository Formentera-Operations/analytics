{{ config(
    materialized='view',
    tags=['wellview', 'job-phases', 'planning', 'performance', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBPROGRAMPHASE') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idwell as well_id,
        idrecparent as parent_record_id,
        idrec as record_id,
        
        -- Phase classification
        code1 as phase_type_1,
        code2 as phase_type_2,
        code3 as phase_type_3,
        code4 as phase_type_4,
        code1234calc as combined_phase_types,
        des as description,
        
        -- Plan information
        durationml as planned_likely_duration_days,
        durationmin as planned_min_duration_days,
        durationmax as planned_max_duration_days,
        durationtechlimit as tech_limit_duration_days,
        costml as planned_likely_phase_cost,
        costmin as planned_min_phase_cost,
        costmax as planned_max_phase_cost,
        costtechlimit as tech_limit_cost,
        
        -- Planned depths (converted to US units)
        depthstartplan / 0.3048 as planned_start_depth_ft,
        depthendplan / 0.3048 as planned_end_depth_ft,
        depthtvdstartplancalc / 0.3048 as planned_start_depth_tvd_ft,
        depthtvdendplancalc / 0.3048 as planned_end_depth_tvd_ft,
        
        -- Actual dates
        dttmstartactual as actual_start_date,
        dttmendactual as actual_end_date,
        dttmendcalc as derived_end_date,
        
        -- Actual depths (converted to US units)
        depthstartactualcalc / 0.3048 as actual_start_depth_ft,
        depthendactualcalc / 0.3048 as actual_end_depth_ft,
        depthtvdstartactualcalc / 0.3048 as actual_start_depth_tvd_ft,
        depthtvdendactualcalc / 0.3048 as actual_end_depth_tvd_ft,
        depthprogressactualcalc / 0.3048 as actual_depth_progress_ft,
        depthprogressplancalc / 0.3048 as planned_depth_progress_ft,
        
        -- Length calculations (converted to US units)
        lengthactualcalc / 0.3048 as actual_phase_length_ft,
        lengthplancalc / 0.3048 as planned_phase_length_ft,
        
        -- Duration calculations (converted to appropriate units)
        durationactualcalc as actual_duration_days,
        durationiltcalc / 0.0416666666666667 as invisible_lost_time_hours,
        durationiltcumcalc / 0.0416666666666667 as cumulative_invisible_lost_time_hours,
        durationnoprobtimecalc / 0.0416666666666667 as time_log_minus_problem_hours,
        durationnoprobtimecumcalc / 0.0416666666666667 as cumulative_time_log_minus_problem_hours,
        durationpersonnelotcalc / 0.0416666666666667 as personnel_ot_hours,
        durationpersonnelregcalc / 0.0416666666666667 as personnel_regular_hours,
        durationpersonneltotcalc / 0.0416666666666667 as personnel_total_hours,
        durationproblemtimecalc / 0.0416666666666667 as problem_time_hours,
        durationproblemtimecumcalc / 0.0416666666666667 as cumulative_problem_time_hours,
        durationtimelogtotalcalc / 0.0416666666666667 as time_log_total_hours,
        durationvariancecalc as duration_variance_days,
        durationvariancecumcalc as cumulative_duration_variance_days,
        
        -- Drilling performance times (converted to hours)
        tmdrillcalc / 0.0416666666666667 as drilling_time_hours,
        tmdrillcumcalc / 0.0416666666666667 as cumulative_drilling_time_hours,
        tmdrillnoexccalc / 0.0416666666666667 as drilling_time_no_exclusions_hours,
        tmdrillcumnoexccalc / 0.0416666666666667 as cumulative_drilling_time_no_exclusions_hours,
        tmcirccalc / 0.0416666666666667 as circulating_time_hours,
        tmcirccumcalc / 0.0416666666666667 as cumulative_circulating_time_hours,
        tmothercalc / 0.0416666666666667 as other_time_hours,
        tmothercumcalc / 0.0416666666666667 as cumulative_other_time_hours,
        tmtripcalc / 0.0416666666666667 as tripping_time_hours,
        tmtripcumcalc / 0.0416666666666667 as cumulative_tripping_time_hours,
        tmrotatingcalc / 0.0416666666666667 as rotating_time_hours,
        tmslidingcalc / 0.0416666666666667 as sliding_time_hours,
        tmcirctripothercalc / 0.0416666666666667 as circ_trip_other_time_hours,
        tmcirctripothercumcalc / 0.0416666666666667 as cumulative_circ_trip_other_time_hours,
        
        -- Sensor durations (converted to minutes)
        duronbtmcalc / 0.000694444444444444 as duration_on_bottom_minutes,
        duroffbtmcalc / 0.000694444444444444 as duration_off_bottom_minutes,
        durpipemovingcalc / 0.000694444444444444 as duration_pipe_moving_minutes,
        
        -- Rate of penetration (converted to ft/hr)
        ropcalc / 7.3152 as rop_ft_per_hour,
        roprotatingcalc / 7.3152 as rop_rotating_ft_per_hour,
        ropslidingcalc / 7.3152 as rop_sliding_ft_per_hour,
        power(nullif(ropinstavgcalc, 0), -1) / 0.00227836103820356 as rop_instantaneous_avg_min_per_ft,
        
        -- Cost calculations
        costactualcalc as actual_phase_field_est,
        costactualcumcalc as actual_phase_cumulative_field_est,
        costactualcumnormcalc as actual_phase_cumulative_field_est_normalized,
        costactualnormcalc as actual_phase_field_est_normalized,
        costmaxcumcalc as max_cumulative_phase_cost,
        costmaxnormcalc as max_phase_cost_normalized,
        costmaxnormcumcalc as max_cumulative_phase_cost_normalized,
        costmincumcalc as min_cumulative_phase_cost,
        costminnormcalc as min_phase_cost_normalized,
        costminnormcumcalc as min_cumulative_phase_cost_normalized,
        costmlcumcalc as likely_cumulative_phase_cost,
        costmlcumnoexcludecalc as likely_cumulative_phase_cost_no_exclusions,
        costmlnormcalc as likely_phase_cost_normalized,
        costmlnormcumcalc as likely_cumulative_phase_cost_normalized,
        costtechlimitcumcalc as tech_limit_cumulative_cost,
        costtechlimitnormcalc as tech_limit_cost_normalized,
        costtechlimitnormcumcalc as tech_limit_cumulative_cost_normalized,
        
        -- Cost per depth (converted to cost/ft)
        costperdepthcalc / 3.28083989501312 as cost_per_depth_drilled_per_ft,
        costperdepthnormcalc / 3.28083989501312 as cost_per_depth_drilled_normalized_per_ft,
        costperdepthplancalc / 3.28083989501312 as planned_cost_per_depth_per_ft,
        costperdepthplannormcalc / 3.28083989501312 as planned_cost_per_depth_normalized_per_ft,
        
        -- Cost variances
        costvariancecalc as cost_variance_ml,
        costvariancecumcalc as cumulative_cost_variance_ml,
        costvariancemaxcalc as cost_variance_max,
        costvariancemaxcumcalc as cumulative_cost_variance_max,
        costvariancemincalc as cost_variance_min,
        costvariancemincumcalc as cumulative_cost_variance_min,
        costvariancetechlimitcalc as cost_variance_tech_limit,
        costvariancetechlimitcumcalc as cumulative_cost_variance_tech_limit,
        
        -- Mud costs (mud cost per depth converted to cost/ft)
        phasemudcostcalc as phase_mud_cost,
        phasemudcostnormcalc as phase_mud_cost_normalized,
        phasemudcostperdepthcalc / 3.28083989501312 as phase_mud_cost_per_depth_per_ft,
        phasemudcostperdepthnormcalc / 3.28083989501312 as phase_mud_cost_per_depth_normalized_per_ft,
        
        -- Percentages (converted from proportion to percentage)
        pctproblemtimecalc / 0.01 as percent_problem_time,
        pctproblemtimecumcalc / 0.01 as cumulative_percent_problem_time,
        percenttmrotatingcalc / 0.01 as percent_time_rotating,
        percenttmslidingcalc / 0.01 as percent_time_sliding,
        ratiodepthactualplancalc / 0.01 as ratio_actual_to_planned_depth_percent,
        
        -- Inclinations (degrees - no conversion needed)
        inclbtmcalc as bottom_inclination_degrees,
        inclmaxcalc as max_inclination_degrees,
        incltopcalc as top_inclination_degrees,
        
        -- Mud density (converted to lb/gal)
        muddensitymaxcalc / 119.826428404623 as max_mud_density_lb_per_gal,
        muddensitymincalc / 119.826428404623 as min_mud_density_lb_per_gal,
        mudtypcalc as mud_type,
        
        -- Volumes (converted to barrels)
        volkicksumcalc / 0.158987294928 as total_kick_volume_bbl,
        vollosssumcalc / 0.158987294928 as total_lost_volume_bbl,
        volmudaddedcalc / 0.158987294928 as mud_added_volume_bbl,
        volmudaddedlossvarcalc / 0.158987294928 as mud_added_minus_losses_bbl,
        volmudlosscalc / 0.158987294928 as mud_losses_volume_bbl,
        
        -- Wellbore size (converted to inches)
        wellboreszcalc / 0.0254 as wellbore_size_inches,
        
        -- Weight (converted to pounds)
        weightmetalrecovtotalcalc / 0.45359237 as weight_metal_recovered_total_lb,
        
        -- Day calculations
        dayjobactualcalc as actual_job_days,
        dayjobmaxplancalc as planned_max_job_days,
        dayjobmaxplanaltcalc as planned_max_job_days_alt,
        dayjobminplancalc as planned_min_job_days,
        dayjobminplanaltcalc as planned_min_job_days_alt,
        dayjobmlplanaltcalc as planned_ml_job_days_alt,
        dayjobmlplancalc as planned_ml_job_days,
        dayjobtechlimitplancalc as planned_tech_limit_job_days,
        dayjobtlplanaltcalc as planned_tl_job_days_alt,
        daysfromspudcalc as days_from_spud,
        dayspudactualcalc as days_from_spud_actual,
        
        -- Planned dates
        dttmendplanmaxcalc as planned_latest_end_date,
        dttmendplanmincalc as planned_earliest_end_date,
        dttmendplanmlcalc as planned_likely_end_date,
        dttmendplantechlimitcalc as planned_tech_limit_end_date,
        dttmstartplanmaxcalc as planned_latest_start_date,
        dttmstartplanmincalc as planned_earliest_start_date,
        dttmstartplanmlcalc as planned_likely_start_date,
        dttmstartplantechlimitcalc as planned_tech_limit_start_date,
        
        -- Activity calculations
        activitydepthendmaxcalc / 0.3048 as activity_end_depth_max_ft,
        activitydepthstartmincalc / 0.3048 as activity_start_depth_min_ft,
        activitydurationmaxcalc as activity_duration_max_days,
        activitydurationmaxcumcalc as activity_duration_max_cumulative_days,
        activitydurationmincalc as activity_duration_min_days,
        activitydurationmincumcalc as activity_duration_min_cumulative_days,
        activitydurationmlcalc as activity_duration_ml_days,
        activitydurationmlcumcalc as activity_duration_ml_cumulative_days,
        
        -- Additional cumulative calculations
        durcumactualcalc as cumulative_actual_duration_days,
        durcumactualstartphasecalc as cumulative_actual_duration_at_phase_start_days,
        durcumflatcalc as cumulative_flat_duration_days,
        durcumslopecalc as cumulative_slope_duration_days,
        durmlcumnoexcludecalc as ml_cumulative_duration_no_exclusions_days,
        durnoprobtimecumdayscalc as cumulative_time_log_minus_problem_days,
        durpersonnelotcumcalc / 0.0416666666666667 as cumulative_personnel_ot_hours,
        durpersonnelregcumcalc / 0.0416666666666667 as cumulative_personnel_regular_hours,
        durpersonneltotcumcalc / 0.0416666666666667 as cumulative_personnel_total_hours,
        durproblemtimecumdayscalc as cumulative_problem_time_days,
        durationspudtoplanmaxcalc / 0.0416666666666667 as duration_spud_to_plan_max_hours,
        durationspudtoplanmincalc / 0.0416666666666667 as duration_spud_to_plan_min_hours,
        durationspudtoplanmlcalc / 0.0416666666666667 as duration_spud_to_plan_ml_hours,
        durationspudtoplantechlcalc / 0.0416666666666667 as duration_spud_to_plan_tech_limit_hours,
        durationtimelogcumspudcalc as cumulative_time_log_days_from_spud,
        durationtimelogtotcumcalc as cumulative_time_log_total_days,
        
        -- Other fields
        definitive as is_definitive,
        exclude as exclude_from_calculations,
        planchange as is_plan_change,
        planphase as plan_details,
        hazards as hazards,
        summary as summary,
        source as source,
        methodtyp1 as method_type,
        methodtyp2 as method_subtype,
        flatorslopecalc as flat_or_slope,
        formationcalc as formation,
        usertxt1 as user_text_1,
        
        -- Reference fields
        idrecwellbore as wellbore_id,
        idrecwellboretk as wellbore_table_key,
        idreclastcascalc as last_casing_string_id,
        idreclastcascalctk as last_casing_string_table_key,
        idreclastrigcalc as last_rig_id,
        idreclastrigcalctk as last_rig_table_key,
        idreclasttubcalc as last_tubing_string_id,
        idreclasttubcalctk as last_tubing_string_table_key,
        
        -- Report calculations
        reportdaycalc as report_day,
        reportnocalc as report_number,
        refnocalc as reference_number,
        
        -- BHA and bit calculations
        bhatotalruncalc as bha_total_runs,
        bitrevscalc as bit_revolutions,
        
        -- Sequence
        sysseq as sequence_number,

        -- System locking fields
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockdate as system_lock_date,

        -- System tracking fields
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,

        -- Fivetran metadata
        _fivetran_synced as fivetran_synced_at

    from source_data
)

select * from renamed