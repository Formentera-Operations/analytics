{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'tubing_rods_equipment']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVTUBCOMP') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as component_id,
        trim(idrecparent)::varchar as tubing_string_id,
        trim(idwell)::varchar as well_id,
        sysseq::int as sequence_number,
        trim(idreclastfailurecalc)::varchar as last_failure_id,
        trim(idreclastfailurecalctk)::varchar as last_failure_table_key,

        -- descriptive fields
        trim(des)::varchar as description,
        trim(iconname)::varchar as icon_name,
        trim(compsubtyp)::varchar as equipment_type,
        itemnocalc::float as item_number,
        trim(desjtcalc)::varchar as description_with_joints,
        joints::float as joint_count,
        jointstallycalc::float as tally_joint_count,
        trim(grade)::varchar as steel_grade,
        trim(material)::varchar as material_type,
        trim(make)::varchar as manufacturer,
        trim(model)::varchar as model,
        trim(sn)::varchar as serial_number,
        trim(usedclass)::varchar as used_class,
        trim(refid)::varchar as reference_id,
        trim(currentstatus)::varchar as current_status,
        trim(currentstatuscalc)::varchar as current_status_calc,
        trim(conditionrun)::varchar as condition_run,
        trim(conditionpull)::varchar as condition_pull,
        trim(coatinginner)::varchar as inner_coating,
        trim(coatingouter)::varchar as outer_coating,
        radioactivesource::boolean as is_radioactive_source,
        linetosurf::boolean as line_to_surface,
        trim(costunitlabel)::varchar as cost_unit_label,
        trim(com)::varchar as comments,

        -- connection information — top
        trim(conntyptop)::varchar as top_connection_type,
        trim(connthrdtop)::varchar as top_connection_thread,
        trim(upsettop)::varchar as top_upset,

        -- connection information — bottom
        trim(conntypbtm)::varchar as bottom_connection_type,
        trim(connthrdbtm)::varchar as bottom_connection_thread,
        trim(upsetbtm)::varchar as bottom_upset,

        -- connection calculations
        trim(connectcalc)::varchar as connection_description,
        trim(connectaltcalc)::varchar as alternative_connection_description,

        -- measurements — dimensions (converted from meters to inches)
        {{ wv_meters_to_inches('szodnom') }} as od_nominal_inches,
        {{ wv_meters_to_inches('szidnom') }} as id_nominal_inches,
        {{ wv_meters_to_inches('szodmax') }} as od_max_inches,
        {{ wv_meters_to_inches('szdrift') }} as drift_diameter_inches,
        {{ wv_meters_to_inches('connsztop') }} as top_connection_size_inches,
        {{ wv_meters_to_inches('connszbtm') }} as bottom_connection_size_inches,
        {{ wv_meters_to_inches('fishneckod') }} as fishing_neck_od_inches,

        -- measurements — linear density (converted from kg/m to lb/ft)
        {{ wv_kgm_to_lb_per_ft('wtperlength') }} as weight_per_foot_lbs,

        -- measurements — lengths (converted from meters to feet)
        {{ wv_meters_to_feet('length') }} as length_ft,
        {{ wv_meters_to_feet('lengthcumcalc') }} as cumulative_length_ft,
        {{ wv_meters_to_feet('lengthtallycalc') }} as tally_length_ft,
        {{ wv_meters_to_feet('fishnecklength') }} as fishing_neck_length_ft,

        -- measurements — depths (converted from meters to feet)
        {{ wv_meters_to_feet('depthtopcalc') }} as top_depth_ft,
        {{ wv_meters_to_feet('depthbtmcalc') }} as bottom_depth_ft,
        {{ wv_meters_to_feet('depthtopcorrected') }} as corrected_top_depth_ft,
        {{ wv_meters_to_feet('depthtvdtopcalc') }} as top_depth_tvd_ft,
        {{ wv_meters_to_feet('depthtvdbtmcalc') }} as bottom_depth_tvd_ft,

        -- measurements — weight (converted from newtons to lbf / klbf)
        {{ wv_newtons_to_lbf('weightcalc') }} as component_weight_lbf,
        {{ wv_newtons_to_klbf('weightcumcalc') }} as cumulative_weight_klbf,

        -- measurements — volumes (converted from cubic meters to barrels)
        {{ wv_cbm_to_bbl('volumeinternalcalc') }} as internal_volume_bbl,
        {{ wv_cbm_to_bbl('volumedispcalc') }} as displaced_volume_bbl,
        {{ wv_cbm_to_bbl('volumedispcumcalc') }} as cumulative_displaced_volume_bbl,

        -- measurements — torque (converted from newton-meters to ft-lbs)
        {{ wv_nm_to_ft_lb('torquemin') }} as makeup_torque_min_ft_lbs,
        {{ wv_nm_to_ft_lb('torquemax') }} as max_torque_ft_lbs,

        -- measurements — pressure (converted from kPa to psi)
        {{ wv_kpa_to_psi('prescollapse') }} as collapse_pressure_psi,
        {{ wv_kpa_to_psi('presburst') }} as burst_pressure_psi,
        {{ wv_kpa_to_psi('presaxialinner') }} as axial_inner_pressure_psi,
        {{ wv_kpa_to_psi('presaxialouter') }} as axial_outer_pressure_psi,

        -- measurements — tensile strength (converted from newtons to klbf)
        {{ wv_newtons_to_klbf('tensilemax') }} as max_tensile_strength_klbf,

        -- measurements — temperature (converted from Celsius to Fahrenheit)
        {{ wv_celsius_to_fahrenheit('temprating') }} as temperature_rating_fahrenheit,

        -- measurements — operational hours (converted from days to hours)
        {{ wv_days_to_hours('hoursstart') }} as starting_hours,

        -- measurements — inclination (degrees, no conversion)
        incltopcalc::float as top_inclination_degrees,
        inclbtmcalc::float as bottom_inclination_degrees,
        inclmaxcalc::float as max_inclination_degrees,

        -- measurements — centralizers
        centralizersnotallycalc::float as centralizer_count,

        -- cost
        cost::float as component_cost,

        -- dates
        dttmmanufacture::timestamp_ntz as manufacture_date,
        dttmstatuscalc::timestamp_ntz as status_date,

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
        and component_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['component_id']) }} as tubing_component_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        tubing_component_sk,

        -- identifiers
        component_id,
        tubing_string_id,
        well_id,
        sequence_number,
        last_failure_id,
        last_failure_table_key,

        -- descriptive fields
        description,
        icon_name,
        equipment_type,
        item_number,
        description_with_joints,
        joint_count,
        tally_joint_count,
        steel_grade,
        material_type,
        manufacturer,
        model,
        serial_number,
        used_class,
        reference_id,
        current_status,
        current_status_calc,
        condition_run,
        condition_pull,
        inner_coating,
        outer_coating,
        is_radioactive_source,
        line_to_surface,
        cost_unit_label,
        comments,

        -- connection information — top
        top_connection_type,
        top_connection_thread,
        top_upset,

        -- connection information — bottom
        bottom_connection_type,
        bottom_connection_thread,
        bottom_upset,

        -- connection calculations
        connection_description,
        alternative_connection_description,

        -- measurements — dimensions
        od_nominal_inches,
        id_nominal_inches,
        od_max_inches,
        drift_diameter_inches,
        top_connection_size_inches,
        bottom_connection_size_inches,
        fishing_neck_od_inches,

        -- measurements — weight per length
        weight_per_foot_lbs,

        -- measurements — lengths
        length_ft,
        cumulative_length_ft,
        tally_length_ft,
        fishing_neck_length_ft,

        -- measurements — depths
        top_depth_ft,
        bottom_depth_ft,
        corrected_top_depth_ft,
        top_depth_tvd_ft,
        bottom_depth_tvd_ft,

        -- measurements — weight
        component_weight_lbf,
        cumulative_weight_klbf,

        -- measurements — volumes
        internal_volume_bbl,
        displaced_volume_bbl,
        cumulative_displaced_volume_bbl,

        -- measurements — torque
        makeup_torque_min_ft_lbs,
        max_torque_ft_lbs,

        -- measurements — pressure
        collapse_pressure_psi,
        burst_pressure_psi,
        axial_inner_pressure_psi,
        axial_outer_pressure_psi,

        -- measurements — tensile strength
        max_tensile_strength_klbf,

        -- measurements — temperature
        temperature_rating_fahrenheit,

        -- measurements — operational hours
        starting_hours,

        -- measurements — inclination
        top_inclination_degrees,
        bottom_inclination_degrees,
        max_inclination_degrees,

        -- measurements — centralizers
        centralizer_count,

        -- cost
        component_cost,

        -- dates
        manufacture_date,
        status_date,

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
