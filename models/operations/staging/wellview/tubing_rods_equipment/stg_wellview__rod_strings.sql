{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'tubing_rods_equipment']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVROD') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as rod_string_id,
        trim(idwell)::varchar as well_id,
        trim(idrecjobrun)::varchar as run_job_id,
        trim(idrecjobruntk)::varchar as run_job_table_key,
        trim(idrecjobpull)::varchar as pull_job_id,
        trim(idrecjobpulltk)::varchar as pull_job_table_key,
        trim(idrecjobprogramphasecalc)::varchar as program_phase_id,
        trim(idrecjobprogramphasecalctk)::varchar as program_phase_table_key,
        trim(idrecwellbore)::varchar as wellbore_id,
        trim(idrecwellboretk)::varchar as wellbore_table_key,
        trim(idrectub)::varchar as tubing_string_id,
        trim(idrectubtk)::varchar as tubing_string_table_key,
        trim(idreclastrigcalc)::varchar as last_rig_id,
        trim(idreclastrigcalctk)::varchar as last_rig_table_key,
        trim(idreclastfailurecalc)::varchar as last_failure_id,
        trim(idreclastfailurecalctk)::varchar as last_failure_table_key,

        -- descriptive fields
        trim(proposedoractual)::varchar as proposed_or_actual,
        propversionno::float as proposed_version_number,
        trim(des)::varchar as rod_string_description,
        trim(com)::varchar as comments,
        trim(gradecalc)::varchar as string_grade,
        trim(connthrdtopcalc)::varchar as top_connection_thread,
        trim(componentscalc)::varchar as string_components_description,
        trim(pullreason)::varchar as pull_reason,
        trim(pullreasondetail)::varchar as pull_reason_detail,
        trim(usertxt1)::varchar as user_text_1,
        trim(usertxt2)::varchar as user_text_2,
        trim(usertxt3)::varchar as user_text_3,
        complexityindex::float as complexity_index,

        -- measurements — depths (converted from meters to feet)
        {{ wv_meters_to_feet('depthbtm') }} as set_depth_ft,
        {{ wv_meters_to_feet('depthtopcalc') }} as top_depth_ft,
        {{ wv_meters_to_feet('depthtvdbtmcalc') }} as set_depth_tvd_ft,
        {{ wv_meters_to_feet('lengthcalc') }} as rod_string_length_ft,
        {{ wv_meters_to_feet('stickupkbcalc') }} as stick_up_kb_ft,

        -- measurements — diameters (converted from meters to inches)
        {{ wv_meters_to_inches('szodnommaxcalc') }} as string_nominal_od_in,
        {{ wv_meters_to_inches('szodnomcompmaxcalc') }} as max_component_nominal_od_in,

        -- measurements — linear density (converted from kg/m to lb/ft)
        {{ wv_kgm_to_lb_per_ft('wtperlengthcalc') }} as weight_per_length_lb_per_ft,

        -- measurements — durations and rates
        durruntopullcalc::float as duration_run_to_pull_days,
        {{ wv_days_to_hours('duronbottomtopickupcalc') }} as duration_on_bottom_to_pickup_hours,
        {{ wv_mps_to_ft_per_hr('depthonbtmtopickupcalc') }} as depth_on_bottom_to_pickup_ft_per_hour,

        -- flags (raw value for enhanced CTE)
        tapered::float as tapered_raw,

        -- dates
        dttmrun::timestamp_ntz as run_datetime,
        dttmpickup::timestamp_ntz as pickup_datetime,
        dttmonbottom::timestamp_ntz as on_bottom_datetime,
        dttmoutofhole::timestamp_ntz as out_of_hole_datetime,
        dttmpull::timestamp_ntz as pull_datetime,
        dttmproppull::timestamp_ntz as proposed_pull_datetime,

        -- system / audit
        syscreatedate::timestamp_ntz as created_at_utc,
        trim(syscreateuser)::varchar as created_by,
        sysmoddate::timestamp_ntz as last_mod_at_utc,
        trim(sysmoduser)::varchar as last_mod_by,
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
        and rod_string_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['rod_string_id']) }} as rod_string_sk,
        *,
        coalesce(tapered_raw = 1, false) as is_tapered_string,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        rod_string_sk,

        -- identifiers
        rod_string_id,
        well_id,
        run_job_id,
        run_job_table_key,
        pull_job_id,
        pull_job_table_key,
        program_phase_id,
        program_phase_table_key,
        wellbore_id,
        wellbore_table_key,
        tubing_string_id,
        tubing_string_table_key,
        last_rig_id,
        last_rig_table_key,
        last_failure_id,
        last_failure_table_key,

        -- descriptive fields
        proposed_or_actual,
        proposed_version_number,
        rod_string_description,
        comments,
        string_grade,
        top_connection_thread,
        string_components_description,
        pull_reason,
        pull_reason_detail,
        user_text_1,
        user_text_2,
        user_text_3,
        complexity_index,

        -- measurements — depths
        set_depth_ft,
        top_depth_ft,
        set_depth_tvd_ft,
        rod_string_length_ft,
        stick_up_kb_ft,

        -- measurements — diameters
        string_nominal_od_in,
        max_component_nominal_od_in,

        -- measurements — weight
        weight_per_length_lb_per_ft,

        -- measurements — durations and rates
        duration_run_to_pull_days,
        duration_on_bottom_to_pickup_hours,
        depth_on_bottom_to_pickup_ft_per_hour,

        -- dates
        run_datetime,
        pickup_datetime,
        on_bottom_datetime,
        out_of_hole_datetime,
        pull_datetime,
        proposed_pull_datetime,

        -- flags
        is_tapered_string,

        -- system / audit
        created_at_utc,
        created_by,
        last_mod_at_utc,
        last_mod_by,
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
