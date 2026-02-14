{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'operations']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBTIMELOG') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as time_log_id,
        trim(idrecparent)::varchar as job_id,
        trim(idwell)::varchar as well_id,

        -- time period
        dttmstart::timestamp_ntz as start_datetime,
        dttmend::timestamp_ntz as end_datetime,

        -- duration calculations (days -> hours)
        {{ wv_days_to_hours('durationcalc') }} as duration_hours,
        {{ wv_days_to_hours('sumofdurationcalc') }} as cumulative_duration_hours,

        -- problem time analysis (days -> hours for detail, days for cumulative)
        {{ wv_days_to_hours('durationproblemtimecalc') }} as problem_time_hours,
        durationproblemtimecumcalc::float as cumulative_problem_time_days,
        {{ wv_days_to_hours('durationnoprobtimecalc') }} as no_problem_time_hours,
        durationnoprobtimecumcalc::float as cumulative_no_problem_time_days,

        -- time log cumulative tracking (in days)
        durationtimelogcumspudcalc::float as cumulative_time_log_spud_days,
        durationtimelogtotcumcalc::float as total_cumulative_time_log_days,

        -- short duration activities (days -> minutes)
        {{ wv_days_to_minutes('duronbtmcalc') }} as on_bottom_duration_minutes,
        {{ wv_days_to_minutes('duroffbtmcalc') }} as off_bottom_duration_minutes,
        {{ wv_days_to_minutes('durpipemovingcalc') }} as pipe_moving_duration_minutes,

        -- activity coding
        trim(code1)::varchar as time_log_code_1,
        trim(code2)::varchar as time_log_code_2,
        trim(code3)::varchar as time_log_code_3,
        trim(code4)::varchar as time_log_code_4,
        trim(code1234calc)::varchar as combined_codes,

        -- operational categorization
        trim(opscategory)::varchar as ops_category,
        trim(unschedtyp)::varchar as unscheduled_type,

        -- depths (meters -> feet)
        {{ wv_meters_to_feet('depthstart') }} as start_depth_ft,
        {{ wv_meters_to_feet('depthend') }} as end_depth_ft,
        {{ wv_meters_to_feet('depthstartdpcalc') }} as start_depth_dp_ft,
        {{ wv_meters_to_feet('depthenddpcalc') }} as end_depth_dp_ft,
        {{ wv_meters_to_feet('depthtvdstartcalc') }} as start_depth_tvd_ft,
        {{ wv_meters_to_feet('depthtvdendcalc') }} as end_depth_tvd_ft,

        -- inclination data (degrees, no conversion)
        inclstartcalc::float as start_inclination_degrees,
        inclendcalc::float as end_inclination_degrees,
        inclmaxcalc::float as max_inclination_degrees,

        -- rate of penetration (meters/sec -> ft/hr)
        {{ wv_mps_to_ft_per_hr('ropcalc') }} as rop_ft_per_hour,

        -- wellbore size (meters -> inches)
        {{ wv_meters_to_inches('wellboreszcalc') }} as wellbore_size_inches,

        -- formation information
        trim(formationcalc)::varchar as formation,

        -- days from spud tracking
        daysfromspudcalc::float as days_from_spud,

        -- report and rig tracking
        reportnocalc::float as report_number,
        rigdayscalc::float as rig_days,
        rigdayscumcalc::float as cumulative_rig_days,
        trim(rigcrewnamecalc)::varchar as rig_crew_name,

        -- problem analysis
        problemcalc::boolean as is_problem_time,
        trim(refnoproblemcalc)::varchar as problem_reference_number,

        -- reference information
        trim(refderrick)::varchar as derrick_reference,

        -- status flags
        inactive::boolean as is_inactive,

        -- foreign keys
        trim(idrecwellbore)::varchar as wellbore_id,
        trim(idrecwellboretk)::varchar as wellbore_table_key,
        trim(idrecjobprogramphasecalc)::varchar as job_program_phase_id,
        trim(idrecjobprogramphasecalctk)::varchar as job_program_phase_table_key,
        trim(idrecjobreportcalc)::varchar as job_report_id,
        trim(idrecjobreportcalctk)::varchar as job_report_table_key,
        trim(idreclastcascalc)::varchar as last_casing_id,
        trim(idreclastcascalctk)::varchar as last_casing_table_key,
        trim(idreclastrigcalc)::varchar as last_rig_id,
        trim(idreclastrigcalctk)::varchar as last_rig_table_key,
        trim(idreclastintprobcalc)::varchar as last_interval_problem_id,
        trim(idreclastintprobcalctk)::varchar as last_interval_problem_table_key,
        trim(idrecwsstring)::varchar as well_servicing_string_id,
        trim(idrecwsstringtk)::varchar as well_servicing_string_table_key,

        -- comments
        trim(com)::varchar as comments,

        -- user fields
        trim(usertxt1)::varchar as user_text_1,
        trim(usertxt2)::varchar as user_text_2,

        -- system / audit
        trim(syscreateuser)::varchar as created_by,
        syscreatedate::timestamp_ntz as created_at,
        trim(sysmoduser)::varchar as modified_by,
        sysmoddate::timestamp_ntz as modified_at,
        trim(systag)::varchar as system_tag,
        syslockmeui::boolean as system_lock_me_ui,
        syslockchildrenui::boolean as system_lock_children_ui,
        syslockme::boolean as system_lock_me,
        syslockchildren::boolean as system_lock_children,
        syslockdate::timestamp_ntz as system_lock_date,

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
        and time_log_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['time_log_id']) }} as job_time_log_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        job_time_log_sk,

        -- identifiers
        time_log_id,
        job_id,
        well_id,

        -- time period
        start_datetime,
        end_datetime,

        -- duration calculations
        duration_hours,
        cumulative_duration_hours,

        -- problem time analysis
        problem_time_hours,
        cumulative_problem_time_days,
        no_problem_time_hours,
        cumulative_no_problem_time_days,

        -- time log cumulative tracking
        cumulative_time_log_spud_days,
        total_cumulative_time_log_days,

        -- short duration activities
        on_bottom_duration_minutes,
        off_bottom_duration_minutes,
        pipe_moving_duration_minutes,

        -- activity coding
        time_log_code_1,
        time_log_code_2,
        time_log_code_3,
        time_log_code_4,
        combined_codes,

        -- operational categorization
        ops_category,
        unscheduled_type,

        -- depths
        start_depth_ft,
        end_depth_ft,
        start_depth_dp_ft,
        end_depth_dp_ft,
        start_depth_tvd_ft,
        end_depth_tvd_ft,

        -- inclination data
        start_inclination_degrees,
        end_inclination_degrees,
        max_inclination_degrees,

        -- rate of penetration
        rop_ft_per_hour,

        -- wellbore size
        wellbore_size_inches,

        -- formation
        formation,

        -- days from spud
        days_from_spud,

        -- report and rig tracking
        report_number,
        rig_days,
        cumulative_rig_days,
        rig_crew_name,

        -- problem analysis
        is_problem_time,
        problem_reference_number,

        -- reference information
        derrick_reference,

        -- status flags
        is_inactive,

        -- foreign keys
        wellbore_id,
        wellbore_table_key,
        job_program_phase_id,
        job_program_phase_table_key,
        job_report_id,
        job_report_table_key,
        last_casing_id,
        last_casing_table_key,
        last_rig_id,
        last_rig_table_key,
        last_interval_problem_id,
        last_interval_problem_table_key,
        well_servicing_string_id,
        well_servicing_string_table_key,

        -- comments
        comments,

        -- user fields
        user_text_1,
        user_text_2,

        -- system / audit
        created_by,
        created_at,
        modified_by,
        modified_at,
        system_tag,
        system_lock_me_ui,
        system_lock_children_ui,
        system_lock_me,
        system_lock_children,
        system_lock_date,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
