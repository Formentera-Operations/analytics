{{ config(
    materialized='view',
    tags=['wellview', 'problems', 'npt', 'interval-analysis', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBINTERVALPROBLEM') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idwell as well_id,
        idrecparent as parent_record_id,
        idrec as record_id,
        
        -- Problem classification
        category as major_category,
        typ as problem_type,
        typdetail as problem_subtype,
        des as description,
        refno as reference_number,
        
        -- Problem timing
        dttmstart as start_date,
        dttmend as end_date,
        dttmstartorcalc as earliest_start_date,
        dttmendorcalc as latest_end_date,
        dateortyp as continuous_or_date_override,
        
        -- Problem location (converted to US units)
        depthstart / 0.3048 as start_depth_ft,
        depthend / 0.3048 as end_depth_ft,
        depthtvdstartcalc / 0.3048 as start_depth_tvd_ft,
        depthtvdendcalc / 0.3048 as end_depth_tvd_ft,
        
        -- Duration calculations (converted to hours)
        durationgrosscalc / 0.0416666666666667 as problem_duration_gross_hours,
        durationnetcalc / 0.0416666666666667 as problem_duration_net_hours,
        estlosttime / 0.0416666666666667 as estimated_lost_time_hours,
        durationfactorcalc as duration_factor,
        
        -- Time log references
        durationtimelogcumspudcalc as cumulative_time_log_days_from_spud,
        durationtimelogtotcumcalc as cumulative_time_log_total_days,
        
        -- Severity and status
        severity as severity,
        potentialseverity as potential_severity,
        status as status,
        opscondition as operative_condition,
        
        -- Cost information
        costcalc as problem_cost,
        costrecov as cost_recovery,
        estcostoverride as estimated_cost_override,
        
        -- Accountability and actions
        accountablepty as accountable_party,
        actiontaken as action_taken,
        
        -- Problem systems
        problemsystem1 as problem_system_1,
        problemsystem2 as problem_system_2,
        problemsystem3 as problem_system_3,
        
        -- Inclinations (degrees - no conversion needed)
        incltopcalc as top_inclination_degrees,
        inclbtmcalc as bottom_inclination_degrees,
        inclmaxcalc as max_inclination_degrees,
        
        -- Formation and wellbore information
        formationcalc as formation,
        idrecwellbore as wellbore_id,
        idrecwellboretk as wellbore_table_key,
        
        -- Phase and equipment references
        idrecjobprogramphasecalc as job_program_phase_id,
        idrecjobprogramphasecalctk as job_program_phase_table_key,
        idrecfaileditem as failed_item_id,
        idrecfaileditemtk as failed_item_table_key,
        idreclastrigcalc as last_rig_id,
        idreclastrigcalctk as last_rig_table_key,
        idrecjobservicecontract as job_service_contract_id,
        idrecjobservicecontracttk as job_service_contract_table_key,
        
        -- Crew and operational information
        rigcrewnamecalc as rig_crew_name,
        
        -- Report timing
        reportdaycalc as report_day,
        reportnocalc as report_number,
        daysfromspudcalc as days_from_spud,
        
        -- Configuration flags
        excludefromproblemtime as exclude_from_problem_time_calculations,
        
        -- Comments
        com as comment,
        
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