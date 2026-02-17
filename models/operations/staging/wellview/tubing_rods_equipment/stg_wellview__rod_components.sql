{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'tubing_rods_equipment']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVRODCOMP') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as rod_component_id,
        trim(idrecparent)::varchar as rod_string_id,
        trim(idwell)::varchar as well_id,
        sysseq::int as sequence_number,
        trim(idreclastfailurecalc)::varchar as last_failure_id,
        trim(idreclastfailurecalctk)::varchar as last_failure_table_key,

        -- descriptive fields
        trim(des)::varchar as component_description,
        trim(desjtcalc)::varchar as description_with_joint_count,
        itemnocalc::float as item_number,
        trim(iconname)::varchar as icon_name,
        trim(compsubtyp)::varchar as equipment_type,
        trim(grade)::varchar as grade,
        joints::float as joint_count,
        trim(material)::varchar as material,
        trim(coating)::varchar as coating,
        trim(make)::varchar as manufacturer,
        trim(model)::varchar as model,
        trim(sn)::varchar as serial_number,
        trim(refid)::varchar as reference_id,
        trim(usedclass)::varchar as condition_class,
        trim(conditionrun)::varchar as condition_when_run,
        trim(conditionpull)::varchar as condition_when_pulled,
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
        trim(connectcalc)::varchar as connections_description,
        trim(connectaltcalc)::varchar as connections_alt_description,

        -- scraper / guide information
        trim(guidedes)::varchar as scraper_description,
        guidesperrod::float as scrapers_per_rod,
        trim(guidematerial)::varchar as scraper_material,
        {{ wv_meters_to_inches('guidesz') }} as scraper_size_in,

        -- measurements — dimensions (converted from meters to inches)
        {{ wv_meters_to_inches('szodnom') }} as nominal_od_in,
        {{ wv_meters_to_inches('szidnom') }} as nominal_id_in,
        {{ wv_meters_to_inches('szodmax') }} as maximum_od_in,
        {{ wv_meters_to_inches('connsztop') }} as top_connection_size_in,
        {{ wv_meters_to_inches('connszbtm') }} as bottom_connection_size_in,
        {{ wv_meters_to_inches('fishneckod') }} as fishing_neck_od_in,

        -- measurements — linear density (converted from kg/m to lb/ft)
        {{ wv_kgm_to_lb_per_ft('wtperlength') }} as weight_per_length_lb_per_ft,

        -- measurements — lengths (converted from meters to feet)
        {{ wv_meters_to_feet('length') }} as length_ft,
        {{ wv_meters_to_feet('lengthcumcalc') }} as cumulative_length_ft,
        {{ wv_meters_to_feet('fishnecklength') }} as fishing_neck_length_ft,

        -- measurements — depths (converted from meters to feet)
        {{ wv_meters_to_feet('depthtopcalc') }} as top_depth_ft,
        {{ wv_meters_to_feet('depthbtmcalc') }} as bottom_depth_ft,
        {{ wv_meters_to_feet('depthtvdtopcalc') }} as top_depth_tvd_ft,
        {{ wv_meters_to_feet('depthtvdbtmcalc') }} as bottom_depth_tvd_ft,
        {{ wv_meters_to_feet('depthtopcorrected') }} as corrected_top_depth_ft,

        -- measurements — weight (converted from newtons to lbf / klbf)
        {{ wv_newtons_to_lbf('weightcalc') }} as component_weight_lbf,
        {{ wv_newtons_to_klbf('weightcumcalc') }} as cumulative_weight_klbf,

        -- measurements — volumes (converted from cubic meters to barrels)
        {{ wv_cbm_to_bbl('volumedispcalc') }} as volume_displaced_bbl,
        {{ wv_cbm_to_bbl('volumedispcumcalc') }} as cumulative_volume_displaced_bbl,

        -- measurements — tensile strength (converted from newtons to klbf)
        {{ wv_newtons_to_klbf('tensilemax') }} as max_tensile_strength_klbf,

        -- measurements — operational hours (converted from days to hours)
        {{ wv_days_to_hours('hoursstart') }} as starting_hours_hr,

        -- measurements — inclination (degrees, no conversion)
        incltopcalc::float as top_inclination_deg,
        inclbtmcalc::float as bottom_inclination_deg,
        inclmaxcalc::float as max_inclination_deg,

        -- cost
        cost::float as item_cost,

        -- dates
        dttmmanufacture::timestamp_ntz as manufacture_date,

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
        and rod_component_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['rod_component_id']) }} as rod_component_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        rod_component_sk,

        -- identifiers
        rod_component_id,
        rod_string_id,
        well_id,
        sequence_number,
        last_failure_id,
        last_failure_table_key,

        -- descriptive fields
        component_description,
        description_with_joint_count,
        item_number,
        icon_name,
        equipment_type,
        grade,
        joint_count,
        material,
        coating,
        manufacturer,
        model,
        serial_number,
        reference_id,
        condition_class,
        condition_when_run,
        condition_when_pulled,
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
        connections_description,
        connections_alt_description,

        -- scraper / guide information
        scraper_description,
        scrapers_per_rod,
        scraper_material,
        scraper_size_in,

        -- measurements — dimensions
        nominal_od_in,
        nominal_id_in,
        maximum_od_in,
        top_connection_size_in,
        bottom_connection_size_in,
        fishing_neck_od_in,

        -- measurements — weight per length
        weight_per_length_lb_per_ft,

        -- measurements — lengths
        length_ft,
        cumulative_length_ft,
        fishing_neck_length_ft,

        -- measurements — depths
        top_depth_ft,
        bottom_depth_ft,
        top_depth_tvd_ft,
        bottom_depth_tvd_ft,
        corrected_top_depth_ft,

        -- measurements — weight
        component_weight_lbf,
        cumulative_weight_klbf,

        -- measurements — volumes
        volume_displaced_bbl,
        cumulative_volume_displaced_bbl,

        -- measurements — tensile strength
        max_tensile_strength_klbf,

        -- measurements — operational hours
        starting_hours_hr,

        -- measurements — inclination
        top_inclination_deg,
        bottom_inclination_deg,
        max_inclination_deg,

        -- cost
        item_cost,

        -- dates
        manufacture_date,

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
