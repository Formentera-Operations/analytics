{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'tubing_rods_equipment']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVTUB') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as tubing_string_id,
        trim(idwell)::varchar as well_id,
        trim(idrecjobrun)::varchar as run_job_id,
        trim(idrecjobruntk)::varchar as run_job_table_key,
        trim(idrecwellbore)::varchar as wellbore_id,
        trim(idrecwellboretk)::varchar as wellbore_table_key,
        trim(idrecjobpull)::varchar as pull_job_id,
        trim(idrecjobpulltk)::varchar as pull_job_table_key,
        trim(idrecstring)::varchar as string_id,
        trim(idrecstringtk)::varchar as string_table_key,
        trim(idreclastrigcalc)::varchar as last_rig_id,
        trim(idreclastrigcalctk)::varchar as last_rig_table_key,
        trim(idreclastfailurecalc)::varchar as last_failure_id,
        trim(idreclastfailurecalctk)::varchar as last_failure_table_key,
        trim(idrecjobprogramphasecalc)::varchar as job_program_phase_id,
        trim(idrecjobprogramphasecalctk)::varchar as job_program_phase_table_key,

        -- descriptive fields
        trim(des)::varchar as description,
        trim(proposedoractual)::varchar as proposed_or_actual,
        propversionno::float as proposed_version_number,
        trim(gradecalc)::varchar as steel_grade,
        trim(connthrdtopcalc)::varchar as top_connection_thread,
        trim(componentscalc)::varchar as components_description,
        trim(comptubdimcalc)::varchar as tapered_dimensions,
        trim(comptubdimszodnomcalc)::varchar as tapered_od_description,
        trim(comptublengthcalc)::varchar as component_length_calc,
        trim(contractor)::varchar as makeup_contractor,
        trim(latposition)::varchar as lateral_position,
        trim(pullreason)::varchar as pull_reason,
        trim(pullreasondetail)::varchar as pull_reason_detail,
        trim(reasoncutpull)::varchar as cut_pull_reason,
        trim(notecutpull)::varchar as cut_pull_note,
        trim(usertxt1)::varchar as user_text_1,
        trim(usertxt2)::varchar as user_text_2,
        trim(usertxt3)::varchar as user_text_3,
        complexityindex::float as complexity_index,
        trim(com)::varchar as comments,

        -- measurements — depths (converted from meters to feet)
        {{ wv_meters_to_feet('depthbtm') }} as set_depth_ft,
        {{ wv_meters_to_feet('depthtopcalc') }} as top_depth_ft,
        {{ wv_meters_to_feet('depthtvdbtmcalc') }} as set_depth_tvd_ft,
        {{ wv_meters_to_feet('lengthcalc') }} as total_length_ft,
        {{ wv_meters_to_feet('stickupkbcalc') }} as stick_up_kb_ft,
        {{ wv_meters_to_feet('depthtoplinkcalc') }} as top_link_depth_ft,
        {{ wv_meters_to_feet('depthbtmlinkcalc') }} as bottom_link_depth_ft,
        {{ wv_meters_to_feet('depthcutpull') }} as cut_pull_depth_ft,
        {{ wv_meters_to_feet('depthtvdcutpullcalc') }} as cut_pull_depth_tvd_ft,
        {{ wv_meters_to_feet('totalstretchsumcalc') }} as total_stretch_ft,

        -- measurements — diameters (converted from meters to inches)
        {{ wv_meters_to_inches('szodnommaxcalc') }} as max_od_inches,
        {{ wv_meters_to_inches('szodnomcompmaxcalc') }} as max_component_od_inches,
        {{ wv_meters_to_inches('szidnommincalc') }} as min_id_inches,
        {{ wv_meters_to_inches('szidnomcompmincalc') }} as min_component_id_inches,
        {{ wv_meters_to_inches('szdriftmincalc') }} as min_drift_inches,

        -- measurements — linear density (converted from kg/m to lb/ft)
        {{ wv_kgm_to_lb_per_ft('wtperlengthcalc') }} as weight_per_foot_lbs,

        -- measurements — tension (converted from newtons to lbf)
        {{ wv_newtons_to_lbf('tension') }} as set_tension_lbf,

        -- measurements — pressure (converted from kPa to psi)
        {{ wv_kpa_to_psi('operatingpresslimit') }} as operating_pressure_limit_psi,

        -- measurements — string weights (converted from newtons to klbf)
        {{ wv_newtons_to_klbf('stringwtup') }} as string_weight_up_klbf,
        {{ wv_newtons_to_klbf('stringwtdown') }} as string_weight_down_klbf,
        {{ wv_newtons_to_klbf('stringwtrotating') }} as string_weight_rotating_klbf,

        -- measurements — volumes (converted from cubic meters to barrels)
        {{ wv_cbm_to_bbl('volumeinternalcalc') }} as internal_volume_bbl,
        {{ wv_cbm_to_bbl('volumeshoetrack') }} as shoe_track_volume_bbl,

        -- measurements — centralizer standoff (proportions to percentages)
        centralizersstandoffavg / 0.01 as avg_standoff_percent,
        centralizersstandoffmin / 0.01 as min_standoff_percent,
        centralizersnotallycalc::float as centralizer_count,

        -- measurements — durations and rates
        durruntopullcalc::float as run_to_pull_duration_days,
        {{ wv_days_to_hours('duronbottomtopickupcalc') }} as on_bottom_to_pickup_duration_hours,
        {{ wv_mps_to_ft_per_hr('depthonbtmtopickupcalc') }} as depth_on_bottom_to_pickup_ft_per_hour,

        -- flags (raw value for enhanced CTE)
        tapered::float as tapered_raw,

        -- dates
        dttmrun::timestamp_ntz as run_date,
        dttmpickup::timestamp_ntz as pickup_date,
        dttmonbottom::timestamp_ntz as on_bottom_date,
        dttmoutofhole::timestamp_ntz as out_of_hole_date,
        dttmpull::timestamp_ntz as pull_date,
        dttmcutpull::timestamp_ntz as cut_pull_date,
        dttmpropcutpull::timestamp_ntz as proposed_cut_pull_date,
        dttmproppull::timestamp_ntz as proposed_pull_date,

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
        and tubing_string_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['tubing_string_id']) }} as tubing_string_sk,
        *,
        coalesce(tapered_raw = 1, false) as is_tapered_string,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        tubing_string_sk,

        -- identifiers
        tubing_string_id,
        well_id,
        run_job_id,
        run_job_table_key,
        wellbore_id,
        wellbore_table_key,
        pull_job_id,
        pull_job_table_key,
        string_id,
        string_table_key,
        last_rig_id,
        last_rig_table_key,
        last_failure_id,
        last_failure_table_key,
        job_program_phase_id,
        job_program_phase_table_key,

        -- descriptive fields
        description,
        proposed_or_actual,
        proposed_version_number,
        steel_grade,
        top_connection_thread,
        components_description,
        tapered_dimensions,
        tapered_od_description,
        component_length_calc,
        makeup_contractor,
        lateral_position,
        pull_reason,
        pull_reason_detail,
        cut_pull_reason,
        cut_pull_note,
        user_text_1,
        user_text_2,
        user_text_3,
        complexity_index,
        comments,

        -- measurements — depths
        set_depth_ft,
        top_depth_ft,
        set_depth_tvd_ft,
        total_length_ft,
        stick_up_kb_ft,
        top_link_depth_ft,
        bottom_link_depth_ft,
        cut_pull_depth_ft,
        cut_pull_depth_tvd_ft,
        total_stretch_ft,

        -- measurements — diameters
        max_od_inches,
        max_component_od_inches,
        min_id_inches,
        min_component_id_inches,
        min_drift_inches,

        -- measurements — weight
        weight_per_foot_lbs,

        -- measurements — tension
        set_tension_lbf,

        -- measurements — pressure
        operating_pressure_limit_psi,

        -- measurements — string weights
        string_weight_up_klbf,
        string_weight_down_klbf,
        string_weight_rotating_klbf,

        -- measurements — volumes
        internal_volume_bbl,
        shoe_track_volume_bbl,

        -- measurements — centralizers
        avg_standoff_percent,
        min_standoff_percent,
        centralizer_count,

        -- measurements — durations and rates
        run_to_pull_duration_days,
        on_bottom_to_pickup_duration_hours,
        depth_on_bottom_to_pickup_ft_per_hour,

        -- dates
        run_date,
        pickup_date,
        on_bottom_date,
        out_of_hole_date,
        pull_date,
        cut_pull_date,
        proposed_cut_pull_date,
        proposed_pull_date,

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
