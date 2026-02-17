{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'operations']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVOTHERINHOLE') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as other_in_hole_id,
        trim(idwell)::varchar as well_id,

        -- basic equipment information
        trim(proposedoractual)::varchar as proposed_or_actual,
        propversionno::int as proposed_version_number,
        trim(des)::varchar as equipment_description,
        trim(compsubtyp)::varchar as equipment_type,
        trim(iconname)::varchar as icon_name,

        -- manufacturer information
        trim(make)::varchar as manufacturer,
        trim(model)::varchar as equipment_model,
        trim(sn)::varchar as serial_number,
        trim(material)::varchar as equipment_material,
        trim(coating)::varchar as equipment_coating,
        trim(refid)::varchar as reference_id,

        -- physical dimensions (converted from meters to inches/feet)
        {{ wv_meters_to_inches('szodnom') }} as nominal_outer_diameter_in,
        {{ wv_meters_to_inches('szidnom') }} as nominal_inner_diameter_in,
        {{ wv_meters_to_inches('szodmax') }} as maximum_outer_diameter_in,
        {{ wv_meters_to_inches('szdrift') }} as drift_diameter_in,
        {{ wv_meters_to_feet('lengthcalc') }} as equipment_length_ft,

        -- fishing specifications (converted from meters to inches/feet)
        {{ wv_meters_to_inches('fishneckod') }} as fishing_neck_od_in,
        {{ wv_meters_to_feet('fishnecklength') }} as fishing_neck_length_ft,

        -- depths and positions (converted from meters to feet)
        {{ wv_meters_to_feet('depthtop') }} as top_depth_ft,
        {{ wv_meters_to_feet('depthbtm') }} as bottom_depth_ft,
        {{ wv_meters_to_feet('depthtvdtopcalc') }} as top_depth_tvd_ft,
        {{ wv_meters_to_feet('depthtvdbtmcalc') }} as bottom_depth_tvd_ft,
        {{ wv_meters_to_feet('depthtopcorrected') }} as corrected_top_depth_ft,
        {{ wv_meters_to_inches('latposition') }} as lateral_position_in,

        -- inclination data (degrees, no conversion needed)
        incltopcalc::float as top_inclination_degrees,
        inclbtmcalc::float as bottom_inclination_degrees,
        inclmaxcalc::float as maximum_inclination_degrees,

        -- tension and pressure ratings (converted to lbf/psi/fahrenheit)
        {{ wv_newtons_to_lbf('tensionpreset') }} as tension_pre_set_lbf,
        {{ wv_newtons_to_lbf('tensionpostset') }} as tension_post_set_lbf,
        {{ wv_kpa_to_psi('presrating') }} as pressure_rating_psi,
        {{ wv_celsius_to_fahrenheit('temprating') }} as temperature_rating_f,

        -- operational dates
        dttmrun::timestamp_ntz as run_datetime,
        dttmpickup::timestamp_ntz as pickup_datetime,
        dttmonbottom::timestamp_ntz as on_bottom_datetime,
        dttmoutofhole::timestamp_ntz as out_of_hole_datetime,
        dttmmanufacture::timestamp_ntz as manufacture_datetime,
        dttmpull::timestamp_ntz as pull_datetime,
        dttmproppull::timestamp_ntz as proposed_pull_datetime,

        -- operational conditions
        trim(conditionrun)::varchar as condition_run,
        trim(conditionpull)::varchar as condition_pull,

        -- status and current condition
        trim(currentstatuscalc)::varchar as current_status,
        dttmstatuscalc::timestamp_ntz as current_status_datetime,

        -- pull information
        trim(pullreason)::varchar as pull_reason,
        trim(pullreasondetail)::varchar as pull_reason_detail,

        -- calculated durations
        durruntopullcalc::float as duration_run_to_pull_days,
        {{ wv_days_to_hours('hoursstart') }} as starting_hours,
        {{ wv_days_to_hours('duronbottomtopickupcalc') }} as duration_on_bottom_to_pickup_hours,
        {{ wv_mps_to_ft_per_hr('depthonbtmtopickupcalc') }} as depth_on_bottom_to_pickup_ft_per_hour,

        -- flags
        coalesce(radioactivesource = 1, false) as is_radioactive_source,

        -- cost information
        cost::float as equipment_cost,
        trim(costunitlabel)::varchar as cost_unit_label,

        -- related entities
        trim(idrecwellbore)::varchar as wellbore_id,
        trim(idrecwellboretk)::varchar as wellbore_table_key,
        trim(idrecstring)::varchar as string_id,
        trim(idrecstringtk)::varchar as string_table_key,
        trim(idrecjobrun)::varchar as run_job_id,
        trim(idrecjobruntk)::varchar as run_job_table_key,
        trim(idrecjobpull)::varchar as pull_job_id,
        trim(idrecjobpulltk)::varchar as pull_job_table_key,
        trim(idrecjobprogramphasecalc)::varchar as program_phase_id,
        trim(idrecjobprogramphasecalctk)::varchar as program_phase_table_key,
        trim(idreclastrigcalc)::varchar as last_rig_id,
        trim(idreclastrigcalctk)::varchar as last_rig_table_key,
        trim(idreclastfailurecalc)::varchar as last_failure_id,
        trim(idreclastfailurecalctk)::varchar as last_failure_table_key,

        -- additional information
        trim(complexityindex)::varchar as complexity_index,
        trim(com)::varchar as comments,

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
        and other_in_hole_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['other_in_hole_id']) }} as other_in_hole_equipment_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        other_in_hole_equipment_sk,

        -- identifiers
        other_in_hole_id,
        well_id,

        -- basic equipment information
        proposed_or_actual,
        proposed_version_number,
        equipment_description,
        equipment_type,
        icon_name,

        -- manufacturer information
        manufacturer,
        equipment_model,
        serial_number,
        equipment_material,
        equipment_coating,
        reference_id,

        -- physical dimensions
        nominal_outer_diameter_in,
        nominal_inner_diameter_in,
        maximum_outer_diameter_in,
        drift_diameter_in,
        equipment_length_ft,

        -- fishing specifications
        fishing_neck_od_in,
        fishing_neck_length_ft,

        -- depths and positions
        top_depth_ft,
        bottom_depth_ft,
        top_depth_tvd_ft,
        bottom_depth_tvd_ft,
        corrected_top_depth_ft,
        lateral_position_in,

        -- inclination data
        top_inclination_degrees,
        bottom_inclination_degrees,
        maximum_inclination_degrees,

        -- tension and pressure ratings
        tension_pre_set_lbf,
        tension_post_set_lbf,
        pressure_rating_psi,
        temperature_rating_f,

        -- operational dates
        run_datetime,
        pickup_datetime,
        on_bottom_datetime,
        out_of_hole_datetime,
        manufacture_datetime,
        pull_datetime,
        proposed_pull_datetime,

        -- operational conditions
        condition_run,
        condition_pull,

        -- status and current condition
        current_status,
        current_status_datetime,

        -- pull information
        pull_reason,
        pull_reason_detail,

        -- calculated durations
        duration_run_to_pull_days,
        starting_hours,
        duration_on_bottom_to_pickup_hours,
        depth_on_bottom_to_pickup_ft_per_hour,

        -- flags
        is_radioactive_source,

        -- cost information
        equipment_cost,
        cost_unit_label,

        -- related entities
        wellbore_id,
        wellbore_table_key,
        string_id,
        string_table_key,
        run_job_id,
        run_job_table_key,
        pull_job_id,
        pull_job_table_key,
        program_phase_id,
        program_phase_table_key,
        last_rig_id,
        last_rig_table_key,
        last_failure_id,
        last_failure_table_key,

        -- additional information
        complexity_index,
        comments,

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
