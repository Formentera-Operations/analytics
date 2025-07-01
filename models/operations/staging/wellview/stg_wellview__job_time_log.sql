{{ config(
    materialized='view',
    tags=['wellview', 'job-time-log', 'drilling', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBTIMELOG') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as time_log_id,
        idrecparent as job_id,
        idwell as well_id,
        
        -- Time period
        dttmstart as start_datetime,
        dttmend as end_datetime,
        
        -- Duration calculations (converted to hours)
        durationcalc / 0.0416666666666667 as duration_hours,
        sumofdurationcalc / 0.0416666666666667 as cumulative_duration_hours,
        
        -- Problem time analysis (converted to hours for detail, days for cumulative)
        durationproblemtimecalc / 0.0416666666666667 as problem_time_hours,
        durationproblemtimecumcalc as cumulative_problem_time_days,
        durationnoprobtimecalc / 0.0416666666666667 as no_problem_time_hours,
        durationnoprobtimecumcalc as cumulative_no_problem_time_days,
        
        -- Time log cumulative tracking (in days)
        durationtimelogcumspudcalc as cumulative_time_log_spud_days,
        durationtimelogtotcumcalc as total_cumulative_time_log_days,
        
        -- Short duration activities (converted to minutes)
        duronbtmcalc / 0.000694444444444444 as on_bottom_duration_minutes,
        duroffbtmcalc / 0.000694444444444444 as off_bottom_duration_minutes,
        durpipemovingcalc / 0.000694444444444444 as pipe_moving_duration_minutes,
        
        -- Activity coding
        code1 as time_log_code_1,
        code2 as time_log_code_2,
        code3 as time_log_code_3,
        code4 as time_log_code_4,
        code1234calc as combined_codes,
        
        -- Operational categorization
        opscategory as ops_category,
        unschedtyp as unscheduled_type,
        
        -- Depths (converted to US units - feet)
        depthstart / 0.3048 as start_depth_ft,
        depthend / 0.3048 as end_depth_ft,
        depthstartdpcalc / 0.3048 as start_depth_dp_ft,
        depthenddpcalc / 0.3048 as end_depth_dp_ft,
        depthtvdstartcalc / 0.3048 as start_depth_tvd_ft,
        depthtvdendcalc / 0.3048 as end_depth_tvd_ft,
        
        -- Inclination data (in degrees)
        inclstartcalc as start_inclination_degrees,
        inclendcalc as end_inclination_degrees,
        inclmaxcalc as max_inclination_degrees,
        
        -- Rate of penetration (converted to US units)
        ropcalc / 7.3152 as rop_ft_per_hour,
        
        -- Wellbore size (converted to inches)
        wellboreszcalc / 0.0254 as wellbore_size_inches,
        
        -- Formation information
        formationcalc as formation,
        
        -- Days from spud tracking
        daysfromspudcalc as days_from_spud,
        
        -- Report and rig tracking
        reportnocalc as report_number,
        rigdayscalc as rig_days,
        rigdayscumcalc as cumulative_rig_days,
        rigcrewnamecalc as rig_crew_name,
        
        -- Problem analysis
        problemcalc as is_problem_time,
        refnoproblemcalc as problem_reference_number,
        
        -- Reference information
        refderrick as derrick_reference,
        
        -- Status flags
        inactive as is_inactive,
        
        -- Foreign key relationships
        idrecwellbore as wellbore_id,
        idrecwellboretk as wellbore_table_key,
        idrecjobprogramphasecalc as job_program_phase_id,
        idrecjobprogramphasecalctk as job_program_phase_table_key,
        idrecjobreportcalc as job_report_id,
        idrecjobreportcalctk as job_report_table_key,
        idreclastcascalc as last_casing_id,
        idreclastcascalctk as last_casing_table_key,
        idreclastrigcalc as last_rig_id,
        idreclastrigcalctk as last_rig_table_key,
        idreclastintprobcalc as last_interval_problem_id,
        idreclastintprobcalctk as last_interval_problem_table_key,
        idrecwsstring as well_servicing_string_id,
        idrecwsstringtk as well_servicing_string_table_key,
        
        -- Comments
        com as comments,
        
        -- User fields
        usertxt1 as user_text_1,
        usertxt2 as user_text_2,
        
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