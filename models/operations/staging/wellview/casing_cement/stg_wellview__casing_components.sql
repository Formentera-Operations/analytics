{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'casing_cement']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran dedup on idrec (one row per casing component)
source as (
    select * from {{ source('wellview_calcs', 'WVT_WVCASCOMP') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

-- 2. RENAMED: Column renaming, type casting, trimming. No filtering, no logic.
renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as component_id,
        trim(idrecparent)::varchar as casing_string_id,
        trim(idwell)::varchar as well_id,
        sysseq::float as component_sequence,

        -- descriptive fields
        trim(des)::varchar as component_description,
        trim(compsubtyp)::varchar as component_subtype,
        trim(iconname)::varchar as icon_name,
        trim(com)::varchar as comments,
        trim(grade)::varchar as component_grade,
        trim(material)::varchar as material_specification,
        trim(make)::varchar as manufacturer,
        trim(model)::varchar as component_model,
        trim(heatrating)::varchar as heat_rating,
        trim(usedclass)::varchar as used_class,
        trim(sn)::varchar as serial_number,
        trim(refid)::varchar as reference_id,
        trim(conntyptop)::varchar as top_connection_type,
        trim(conntypbtm)::varchar as bottom_connection_type,
        trim(connthrdtop)::varchar as top_connection_thread,
        trim(connthrdbtm)::varchar as bottom_connection_thread,
        trim(conntgtperftop)::varchar as top_connection_target_performance,
        trim(conntgtperfbtm)::varchar as bottom_connection_target_performance,
        trim(upsettop)::varchar as top_upset,
        trim(upsetbtm)::varchar as bottom_upset,
        trim(connectcalc)::varchar as connection_info,
        trim(connectaltcalc)::varchar as connection_info_alt,
        trim(costunitlabel)::varchar as cost_unit_label,
        trim(itemnocalc)::varchar as item_number,
        trim(desjtcalc)::varchar as description_with_joints,
        trim(currentstatuscalc)::varchar as current_status,
        trim(idreclastfailurecalc)::varchar as last_failure_id,
        trim(idreclastfailurecalctk)::varchar as last_failure_table_key,

        -- inclinations
        incltopcalc::float as top_inclination_deg,
        inclbtmcalc::float as bottom_inclination_deg,
        inclmaxcalc::float as max_inclination_deg,

        -- joints and quantities
        joints::float as joint_count,
        jointstallycalc::float as joints_in_tally,
        centralizersnotallycalc::float as centralizer_count_tally,

        -- sizes (converted from metric to US units)
        {{ wv_meters_to_inches('szodnom') }} as nominal_od_in,
        {{ wv_meters_to_inches('szidnom') }} as nominal_id_in,
        {{ wv_meters_to_inches('szodmax') }} as max_od_in,
        {{ wv_meters_to_inches('szdrift') }} as drift_diameter_in,
        {{ wv_meters_to_inches('connsztop') }} as top_connection_size_in,
        {{ wv_meters_to_inches('connszbtm') }} as bottom_connection_size_in,

        -- depths (converted from metric to US units)
        {{ wv_meters_to_feet('length') }} as component_length_ft,
        {{ wv_meters_to_feet('depthtopcalc') }} as top_depth_ft,
        {{ wv_meters_to_feet('depthbtmcalc') }} as bottom_depth_ft,
        {{ wv_meters_to_feet('depthtopcorrected') }} as top_depth_corrected_ft,
        {{ wv_meters_to_feet('depthtvdtopcalc') }} as top_depth_tvd_ft,
        {{ wv_meters_to_feet('depthtvdbtmcalc') }} as bottom_depth_tvd_ft,
        {{ wv_meters_to_feet('lengthcumcalc') }} as cumulative_length_ft,
        {{ wv_meters_to_feet('lengthtallycalc') }} as tally_length_ft,

        -- weights and forces (converted from metric to US units)
        {{ wv_kgm_to_lb_per_ft('wtperlength') }} as weight_per_length_lb_per_ft,
        {{ wv_newtons_to_klbf('weightcalc') }} as component_weight_kips,
        {{ wv_newtons_to_klbf('weightcumcalc') }} as cumulative_weight_klbf,
        {{ wv_newtons_to_klbf('tensilemax') }} as max_tensile_strength_klbf,

        -- torque (converted from metric to US units)
        {{ wv_nm_to_ft_lb('torquemin') }} as min_makeup_torque_ft_lb,
        {{ wv_nm_to_ft_lb('torquemax') }} as max_torque_ft_lb,

        -- pressures (converted from metric to US units)
        {{ wv_kpa_to_psi('presburst') }} as burst_pressure_psi,
        {{ wv_kpa_to_psi('prescollapse') }} as collapse_pressure_psi,
        {{ wv_kpa_to_psi('presaxialinner') }} as axial_inner_pressure_psi,
        {{ wv_kpa_to_psi('presaxialouter') }} as axial_outer_pressure_psi,

        -- volumes (converted from metric to US units)
        {{ wv_cbm_to_bbl('volumeinternalcalc') }} as internal_volume_bbl,
        {{ wv_cbm_to_bbl('volumedispcalc') }} as displaced_volume_bbl,
        {{ wv_cbm_to_bbl('volumedispcumcalc') }} as cumulative_displaced_volume_bbl,

        -- cost
        cost::float as component_cost,

        -- dates
        dttmmanufacture::timestamp_ntz as manufacture_datetime,
        dttmstatuscalc::timestamp_ntz as status_datetime,

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
        and component_id is not null
),

-- 4. ENHANCED: Add surrogate key and _loaded_at.
enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['component_id']) }} as casing_component_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        casing_component_sk,

        -- identifiers
        component_id,
        casing_string_id,
        well_id,
        component_sequence,

        -- descriptive fields
        component_description,
        component_subtype,
        icon_name,
        comments,
        component_grade,
        material_specification,
        manufacturer,
        component_model,
        heat_rating,
        used_class,
        serial_number,
        reference_id,
        top_connection_type,
        bottom_connection_type,
        top_connection_thread,
        bottom_connection_thread,
        top_connection_target_performance,
        bottom_connection_target_performance,
        top_upset,
        bottom_upset,
        connection_info,
        connection_info_alt,
        cost_unit_label,
        item_number,
        description_with_joints,
        current_status,
        last_failure_id,
        last_failure_table_key,

        -- inclinations
        top_inclination_deg,
        bottom_inclination_deg,
        max_inclination_deg,

        -- joints and quantities
        joint_count,
        joints_in_tally,
        centralizer_count_tally,

        -- sizes
        nominal_od_in,
        nominal_id_in,
        max_od_in,
        drift_diameter_in,
        top_connection_size_in,
        bottom_connection_size_in,

        -- depths
        component_length_ft,
        top_depth_ft,
        bottom_depth_ft,
        top_depth_corrected_ft,
        top_depth_tvd_ft,
        bottom_depth_tvd_ft,
        cumulative_length_ft,
        tally_length_ft,

        -- weights and forces
        weight_per_length_lb_per_ft,
        component_weight_kips,
        cumulative_weight_klbf,
        max_tensile_strength_klbf,

        -- torque
        min_makeup_torque_ft_lb,
        max_torque_ft_lb,

        -- pressures
        burst_pressure_psi,
        collapse_pressure_psi,
        axial_inner_pressure_psi,
        axial_outer_pressure_psi,

        -- volumes
        internal_volume_bbl,
        displaced_volume_bbl,
        cumulative_displaced_volume_bbl,

        -- cost
        component_cost,

        -- dates
        manufacture_datetime,
        status_datetime,

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
