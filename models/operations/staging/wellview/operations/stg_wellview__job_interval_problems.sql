{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'operations']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBINTERVALPROBLEM') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as record_id,
        trim(idwell)::varchar as well_id,
        trim(idrecparent)::varchar as parent_record_id,
        trim(idrecwellbore)::varchar as wellbore_id,
        trim(idrecwellboretk)::varchar as wellbore_table_key,
        trim(idrecjobprogramphasecalc)::varchar as job_program_phase_id,
        trim(idrecjobprogramphasecalctk)::varchar as job_program_phase_table_key,
        trim(idrecfaileditem)::varchar as failed_item_id,
        trim(idrecfaileditemtk)::varchar as failed_item_table_key,
        trim(idreclastrigcalc)::varchar as last_rig_id,
        trim(idreclastrigcalctk)::varchar as last_rig_table_key,
        trim(idrecjobservicecontract)::varchar as job_service_contract_id,
        trim(idrecjobservicecontracttk)::varchar as job_service_contract_table_key,

        -- descriptive fields
        trim(category)::varchar as major_category,
        trim(typ)::varchar as problem_type,
        trim(typdetail)::varchar as problem_subtype,
        trim(des)::varchar as description,
        trim(refno)::varchar as reference_number,
        trim(dateortyp)::varchar as continuous_or_date_override,
        trim(severity)::varchar as severity,
        trim(potentialseverity)::varchar as potential_severity,
        trim(status)::varchar as status,
        trim(opscondition)::varchar as operative_condition,
        trim(accountablepty)::varchar as accountable_party,
        trim(actiontaken)::varchar as action_taken,
        trim(problemsystem1)::varchar as problem_system_1,
        trim(problemsystem2)::varchar as problem_system_2,
        trim(problemsystem3)::varchar as problem_system_3,
        trim(formationcalc)::varchar as formation,
        trim(rigcrewnamecalc)::varchar as rig_crew_name,
        trim(com)::varchar as comment,

        -- dates
        dttmstart::timestamp_ntz as start_date,
        dttmend::timestamp_ntz as end_date,
        dttmstartorcalc::timestamp_ntz as earliest_start_date,
        dttmendorcalc::timestamp_ntz as latest_end_date,

        -- measurements: depth
        {{ wv_meters_to_feet('depthstart') }} as start_depth_ft,
        {{ wv_meters_to_feet('depthend') }} as end_depth_ft,
        {{ wv_meters_to_feet('depthtvdstartcalc') }} as start_depth_tvd_ft,
        {{ wv_meters_to_feet('depthtvdendcalc') }} as end_depth_tvd_ft,

        -- measurements: duration
        {{ wv_days_to_hours('durationgrosscalc') }} as problem_duration_gross_hours,
        {{ wv_days_to_hours('durationnetcalc') }} as problem_duration_net_hours,
        {{ wv_days_to_hours('estlosttime') }} as estimated_lost_time_hours,
        durationfactorcalc::float as duration_factor,

        -- measurements: time log references
        durationtimelogcumspudcalc::float as cumulative_time_log_days_from_spud,
        durationtimelogtotcumcalc::float as cumulative_time_log_total_days,

        -- measurements: inclination (degrees - no conversion needed)
        incltopcalc::float as top_inclination_degrees,
        inclbtmcalc::float as bottom_inclination_degrees,
        inclmaxcalc::float as max_inclination_degrees,

        -- measurements: cost
        costcalc::float as problem_cost,
        costrecov::float as cost_recovery,
        estcostoverride::float as estimated_cost_override,

        -- measurements: reporting
        reportdaycalc::float as report_day,
        reportnocalc::float as report_number,
        daysfromspudcalc::float as days_from_spud,

        -- flags
        excludefromproblemtime::boolean as exclude_from_problem_time_calculations,

        -- sequence
        sysseq::int as sequence_number,

        -- system / audit
        trim(syscreateuser)::varchar as created_by,
        syscreatedate::timestamp_ntz as created_at,
        trim(sysmoduser)::varchar as modified_by,
        sysmoddate::timestamp_ntz as modified_at,
        trim(systag)::varchar as system_tag,
        syslockdate::timestamp_ntz as system_lock_date,
        syslockme::boolean as system_lock_me,
        syslockchildren::boolean as system_lock_children,
        syslockmeui::boolean as system_lock_me_ui,
        syslockchildrenui::boolean as system_lock_children_ui,

        -- ingestion metadata
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_tz as _fivetran_synced

    from source
),

filtered as (
    select *
    from renamed
    where
        coalesce(_fivetran_deleted, false) = false
        and record_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['record_id']) }} as job_interval_problem_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        job_interval_problem_sk,

        -- identifiers
        record_id,
        well_id,
        parent_record_id,
        wellbore_id,
        wellbore_table_key,
        job_program_phase_id,
        job_program_phase_table_key,
        failed_item_id,
        failed_item_table_key,
        last_rig_id,
        last_rig_table_key,
        job_service_contract_id,
        job_service_contract_table_key,

        -- descriptive fields
        major_category,
        problem_type,
        problem_subtype,
        description,
        reference_number,
        continuous_or_date_override,
        severity,
        potential_severity,
        status,
        operative_condition,
        accountable_party,
        action_taken,
        problem_system_1,
        problem_system_2,
        problem_system_3,
        formation,
        rig_crew_name,
        comment,

        -- dates
        start_date,
        end_date,
        earliest_start_date,
        latest_end_date,

        -- measurements: depth
        start_depth_ft,
        end_depth_ft,
        start_depth_tvd_ft,
        end_depth_tvd_ft,

        -- measurements: duration
        problem_duration_gross_hours,
        problem_duration_net_hours,
        estimated_lost_time_hours,
        duration_factor,

        -- measurements: time log references
        cumulative_time_log_days_from_spud,
        cumulative_time_log_total_days,

        -- measurements: inclination
        top_inclination_degrees,
        bottom_inclination_degrees,
        max_inclination_degrees,

        -- measurements: cost
        problem_cost,
        cost_recovery,
        estimated_cost_override,

        -- measurements: reporting
        report_day,
        report_number,
        days_from_spud,

        -- flags
        exclude_from_problem_time_calculations,

        -- sequence
        sequence_number,

        -- system / audit
        created_by,
        created_at,
        modified_by,
        modified_at,
        system_tag,
        system_lock_date,
        system_lock_me,
        system_lock_children,
        system_lock_me_ui,
        system_lock_children_ui,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
