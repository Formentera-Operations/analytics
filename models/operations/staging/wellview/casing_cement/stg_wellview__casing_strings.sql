{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'casing_cement']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran dedup on idrec (one row per casing string)
source as (
    select * from {{ source('wellview_calcs', 'WVT_WVCAS') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

-- 2. RENAMED: Column renaming, type casting, trimming. No filtering, no logic.
renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as casing_string_id,
        trim(idwell)::varchar as well_id,
        trim(idrecwellbore)::varchar as wellbore_id,
        trim(idrecwellboretk)::varchar as wellbore_table_key,

        -- job identifiers
        trim(idrecjobrun)::varchar as run_job_id,
        trim(idrecjobruntk)::varchar as run_job_table_key,
        trim(idrecjobpull)::varchar as pull_job_id,
        trim(idrecjobpulltk)::varchar as pull_job_table_key,
        trim(idrecjobprogramphasecalc)::varchar as program_phase_id,
        trim(idrecjobprogramphasecalctk)::varchar as program_phase_table_key,

        -- descriptive fields
        trim(des)::varchar as casing_description,
        trim(proposedoractual)::varchar as proposed_or_actual,
        propversionno::float as proposed_version_number,
        trim(com)::varchar as comments,
        trim(gradecalc)::varchar as string_grade,
        trim(connthrdtopcalc)::varchar as top_connection_thread,
        trim(componentscalc)::varchar as string_components,
        trim(compcasdimcalc)::varchar as tapered_string_dimensions,
        trim(compcasdimszodnomcalc)::varchar as tapered_string_od_nominal,
        trim(compcaslengthcalc)::varchar as component_casing_length,
        centralizers::float as centralizers,
        scratchers::float as scratchers,
        centralizersnotallycalc::float as centralizer_count_tally,
        trim(contractor)::varchar as makeup_contractor,
        trim(latposition)::varchar as lateral_position,
        trim(idreclastrigcalc)::varchar as last_rig_id,
        trim(idreclastrigcalctk)::varchar as last_rig_table_key,
        trim(idreclastfailurecalc)::varchar as last_failure_id,
        trim(idreclastfailurecalctk)::varchar as last_failure_table_key,
        complexityindex::float as complexity_index,

        -- depths and measurements (converted from metric to US units)
        {{ wv_meters_to_feet('depthbtm') }} as set_depth_ft,
        {{ wv_meters_to_feet('depthtopcalc') }} as top_depth_ft,
        {{ wv_meters_to_feet('depthtvdbtmcalc') }} as set_depth_tvd_ft,
        {{ wv_meters_to_feet('lengthcalc') }} as string_length_ft,
        {{ wv_meters_to_feet('stickupkbcalc') }} as stick_up_kb_ft,
        {{ wv_meters_to_feet('depthcutpull') }} as cut_pull_depth_ft,
        {{ wv_meters_to_feet('depthtvdcutpullcalc') }} as cut_pull_depth_tvd_ft,
        {{ wv_meters_to_feet('totalstretchsumcalc') }} as total_stretch_ft,

        -- sizes (converted from metric to US units)
        {{ wv_meters_to_inches('szodnommaxcalc') }} as max_nominal_od_in,
        {{ wv_meters_to_inches('szodnomcompmaxcalc') }} as max_component_nominal_od_in,
        {{ wv_meters_to_inches('szidnommincalc') }} as min_nominal_id_in,
        {{ wv_meters_to_inches('szidnomcompmincalc') }} as min_component_nominal_id_in,
        {{ wv_meters_to_inches('szdriftmincalc') }} as min_drift_size_in,
        {{ wv_meters_to_inches('wellboreszcalc') }} as wellbore_size_in,

        -- weights and forces (converted from metric to US units)
        {{ wv_kgm_to_lb_per_ft('wtperlengthcalc') }} as weight_per_length_lb_per_ft,
        {{ wv_newtons_to_klbf('stringwtup') }} as string_weight_up_klbf,
        {{ wv_newtons_to_klbf('stringwtdown') }} as string_weight_down_klbf,
        {{ wv_newtons_to_klbf('travelequipwt') }} as travel_equipment_weight_klbf,
        {{ wv_newtons_to_klbf('tension') }} as set_tension_kips,

        -- volumes (converted from metric to US units)
        {{ wv_cbm_to_bbl('volumeinternalcalc') }} as internal_volume_bbl,
        {{ wv_cbm_to_bbl('volumeshoetrack') }} as shoe_track_volume_bbl,

        -- pressures (converted from metric to US units)
        {{ wv_kpa_to_psi('operatingpresslimit') }} as operating_pressure_limit_psi,
        {{ wv_kpa_to_psi('leakoffprescalc') }} as leak_off_pressure_psi,

        -- density (converted from metric to US units)
        {{ wv_kgm3_to_lb_per_gal('leakoffdensityfluidcalc') }} as leak_off_fluid_density_ppg,

        -- percentages
        centralizersstandoffavg / 0.01 as centralizer_standoff_avg_percent,
        centralizersstandoffmin / 0.01 as centralizer_standoff_min_percent,

        -- speed (converted from metric to US units)
        {{ wv_mps_to_ft_per_hr('depthonbtmtopickupcalc') }} as depth_on_bottom_to_pickup_ft_per_hour,

        -- duration (converted from metric to US units)
        {{ wv_days_to_hours('duronbottomtopickupcalc') }} as duration_on_bottom_to_pickup_hours,
        durruntopullcalc::float as duration_run_to_pull_days,

        -- tapered flag raw value
        tapered::float as tapered_raw,

        -- dates
        dttmrun::timestamp_ntz as run_datetime,
        dttmpickup::timestamp_ntz as pickup_datetime,
        dttmonbottom::timestamp_ntz as on_bottom_datetime,
        dttmoutofhole::timestamp_ntz as out_of_hole_datetime,
        dttmpull::timestamp_ntz as pull_datetime,
        dttmproppull::timestamp_ntz as proposed_pull_datetime,
        dttmcutpull::timestamp_ntz as cut_pull_datetime,
        dttmpropcutpull::timestamp_ntz as proposed_cut_pull_datetime,

        -- pull details
        trim(pullreason)::varchar as pull_reason,
        trim(pullreasondetail)::varchar as pull_reason_detail,
        trim(reasoncutpull)::varchar as cut_pull_reason,
        trim(notecutpull)::varchar as cut_pull_note,

        -- user fields
        trim(usertxt1)::varchar as user_text_1,
        trim(usertxt2)::varchar as user_text_2,
        trim(usertxt3)::varchar as user_text_3,

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

-- 3. FILTERED: Remove soft deletes and null PKs. No transformations.
filtered as (
    select *
    from renamed
    where
        coalesce(_fivetran_deleted, false) = false
        and casing_string_id is not null
),

-- 4. ENHANCED: Add surrogate key, computed flags, and _loaded_at.
enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['casing_string_id']) }} as casing_string_sk,
        *,
        coalesce(tapered_raw = 1, false) as is_tapered,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        casing_string_sk,

        -- identifiers
        casing_string_id,
        well_id,
        wellbore_id,
        wellbore_table_key,

        -- job identifiers
        run_job_id,
        run_job_table_key,
        pull_job_id,
        pull_job_table_key,
        program_phase_id,
        program_phase_table_key,

        -- descriptive fields
        casing_description,
        proposed_or_actual,
        proposed_version_number,
        comments,
        string_grade,
        top_connection_thread,
        string_components,
        tapered_string_dimensions,
        tapered_string_od_nominal,
        component_casing_length,
        centralizers,
        scratchers,
        centralizer_count_tally,
        makeup_contractor,
        lateral_position,
        last_rig_id,
        last_rig_table_key,
        last_failure_id,
        last_failure_table_key,
        complexity_index,

        -- depths and measurements
        set_depth_ft,
        top_depth_ft,
        set_depth_tvd_ft,
        string_length_ft,
        stick_up_kb_ft,
        cut_pull_depth_ft,
        cut_pull_depth_tvd_ft,
        total_stretch_ft,

        -- sizes
        max_nominal_od_in,
        max_component_nominal_od_in,
        min_nominal_id_in,
        min_component_nominal_id_in,
        min_drift_size_in,
        wellbore_size_in,

        -- weights and forces
        weight_per_length_lb_per_ft,
        string_weight_up_klbf,
        string_weight_down_klbf,
        travel_equipment_weight_klbf,
        set_tension_kips,

        -- volumes
        internal_volume_bbl,
        shoe_track_volume_bbl,

        -- pressures
        operating_pressure_limit_psi,
        leak_off_pressure_psi,

        -- density
        leak_off_fluid_density_ppg,

        -- percentages
        centralizer_standoff_avg_percent,
        centralizer_standoff_min_percent,

        -- speed
        depth_on_bottom_to_pickup_ft_per_hour,

        -- duration
        duration_on_bottom_to_pickup_hours,
        duration_run_to_pull_days,

        -- dates
        run_datetime,
        pickup_datetime,
        on_bottom_datetime,
        out_of_hole_datetime,
        pull_datetime,
        proposed_pull_datetime,
        cut_pull_datetime,
        proposed_cut_pull_datetime,

        -- pull details
        pull_reason,
        pull_reason_detail,
        cut_pull_reason,
        cut_pull_note,

        -- user fields
        user_text_1,
        user_text_2,
        user_text_3,

        -- flags
        is_tapered,

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
