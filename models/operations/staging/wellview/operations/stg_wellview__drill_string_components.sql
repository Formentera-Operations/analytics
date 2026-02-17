{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'operations']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBDRILLSTRINGCOMP') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as drill_string_component_id,
        trim(idrecparent)::varchar as drill_string_id,
        trim(idwell)::varchar as well_id,
        sysseq::int as sequence_number,

        -- basic component information
        trim(des)::varchar as item_description,
        trim(iconname)::varchar as icon_name,
        trim(compsubtyp)::varchar as equipment_type,
        trim(grade)::varchar as grade,
        joints::int as number_of_joints,
        jointstallycalc::int as joints_in_tally,
        itemnocalc::int as item_number,
        trim(desjtcalc)::varchar as description_with_joints,

        -- physical dimensions (converted from meters to inches/feet)
        {{ wv_meters_to_inches('szodnom') }} as nominal_od_in,
        {{ wv_meters_to_inches('szidnom') }} as nominal_id_in,
        {{ wv_meters_to_inches('szodmax') }} as max_od_in,
        {{ wv_meters_to_inches('szdrift') }} as drift_in,
        {{ wv_meters_to_feet('length') }} as length_ft,
        {{ wv_meters_to_feet('lengthcumcalc') }} as cumulative_length_ft,
        {{ wv_meters_to_feet('lengthtallycalc') }} as tally_length_ft,

        -- fishing neck dimensions (converted from meters to inches/feet)
        {{ wv_meters_to_inches('fishneckod') }} as fishing_neck_od_in,
        {{ wv_meters_to_feet('fishnecklength') }} as fishing_neck_length_ft,

        -- weight specifications (converted from kg/m to lb/ft, N to lbf/klbf)
        {{ wv_kgm_to_lb_per_ft('wtperlength') }} as weight_per_length_lb_per_ft,
        {{ wv_newtons_to_lbf('weightcalc') }} as component_weight_lbf,
        {{ wv_newtons_to_klbf('weightcumcalc') }} as cumulative_weight_klbf,

        -- volume calculations (converted from m3 to bbl)
        {{ wv_cbm_to_bbl('volumeinternalcalc') }} as internal_volume_bbl,
        {{ wv_cbm_to_bbl('volumedispcumcalc') }} as cumulative_volume_displaced_bbl,

        -- connection specifications (converted from meters to inches)
        trim(conntyptop)::varchar as top_connection_type,
        trim(connthrdtop)::varchar as top_connection_thread,
        trim(upsettop)::varchar as top_upset,
        {{ wv_meters_to_inches('connsztop') }} as top_connection_size_in,
        trim(conntypbtm)::varchar as bottom_connection_type,
        trim(connthrdbtm)::varchar as bottom_connection_thread,
        trim(upsetbtm)::varchar as bottom_upset,
        {{ wv_meters_to_inches('connszbtm') }} as bottom_connection_size_in,
        trim(connectcalc)::varchar as connections,
        trim(connectaltcalc)::varchar as connections_alt_format,

        -- torque specifications (converted from Nm to ft-lb)
        {{ wv_nm_to_ft_lb('torquemin') }} as makeup_torque_ft_lb,
        {{ wv_nm_to_ft_lb('torquemax') }} as max_torque_ft_lb,

        -- ratings and specifications
        {{ wv_newtons_to_klbf('tensilemax') }} as max_tensile_strength_klbf,
        {{ wv_celsius_to_fahrenheit('temprating') }} as temperature_rating_deg_f,

        -- manufacturer information
        trim(make)::varchar as manufacturer,
        trim(model)::varchar as model,
        trim(sn)::varchar as serial_number,
        trim(material)::varchar as material,
        trim(coating)::varchar as coating,
        trim(service)::varchar as service_type,
        trim(owner)::varchar as owner,
        trim(refid)::varchar as reference_id,

        -- condition and status
        trim(usedclass)::varchar as condition_class,
        trim(conditionrun)::varchar as condition_run,
        trim(conditionpull)::varchar as condition_pull,
        trim(currentstatus)::varchar as current_status,
        trim(currentstatuscalc)::varchar as current_status_calculated,
        dttmlastinspect::timestamp_ntz as last_inspection_date,
        dayssinceinspectcalc::float as days_since_last_inspection,
        dttmmanufacture::timestamp_ntz as manufacture_date,
        dttmstatuscalc::timestamp_ntz as current_status_date,
        comptotalruncalc::int as total_number_of_runs,

        -- special equipment features
        radioactivesource::boolean as radioactive_source,
        trim(linetosurf)::varchar as line_to_surface,
        centralizersnotallycalc::int as number_of_centralizers_tally,

        -- operational performance (converted from days to hours, meters to feet)
        {{ wv_days_to_hours('hoursstart') }} as starting_hours_hr,
        {{ wv_days_to_hours('hoursendcalc') }} as ending_hours_hr,
        {{ wv_meters_to_feet('depthdrilledjobcalc') }} as depth_drilled_this_job_ft,
        {{ wv_days_to_hours('tmdrilledjobcalc') }} as drilling_time_this_job_hr,
        {{ wv_days_to_hours('tmcircjobcalc') }} as circulating_time_this_job_hr,

        -- cost information
        cost::float as item_cost,
        trim(costunitlabel)::varchar as cost_unit_label,

        -- comments
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
        and drill_string_component_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['drill_string_component_id']) }} as drill_string_component_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        drill_string_component_sk,

        -- identifiers
        drill_string_component_id,
        drill_string_id,
        well_id,
        sequence_number,

        -- basic component information
        item_description,
        icon_name,
        equipment_type,
        grade,
        number_of_joints,
        joints_in_tally,
        item_number,
        description_with_joints,

        -- physical dimensions
        nominal_od_in,
        nominal_id_in,
        max_od_in,
        drift_in,
        length_ft,
        cumulative_length_ft,
        tally_length_ft,

        -- fishing neck dimensions
        fishing_neck_od_in,
        fishing_neck_length_ft,

        -- weight specifications
        weight_per_length_lb_per_ft,
        component_weight_lbf,
        cumulative_weight_klbf,

        -- volume calculations
        internal_volume_bbl,
        cumulative_volume_displaced_bbl,

        -- connection specifications
        top_connection_type,
        top_connection_thread,
        top_upset,
        top_connection_size_in,
        bottom_connection_type,
        bottom_connection_thread,
        bottom_upset,
        bottom_connection_size_in,
        connections,
        connections_alt_format,

        -- torque specifications
        makeup_torque_ft_lb,
        max_torque_ft_lb,

        -- ratings and specifications
        max_tensile_strength_klbf,
        temperature_rating_deg_f,

        -- manufacturer information
        manufacturer,
        model,
        serial_number,
        material,
        coating,
        service_type,
        owner,
        reference_id,

        -- condition and status
        condition_class,
        condition_run,
        condition_pull,
        current_status,
        current_status_calculated,
        last_inspection_date,
        days_since_last_inspection,
        manufacture_date,
        current_status_date,
        total_number_of_runs,

        -- special equipment features
        radioactive_source,
        line_to_surface,
        number_of_centralizers_tally,

        -- operational performance
        starting_hours_hr,
        ending_hours_hr,
        depth_drilled_this_job_ft,
        drilling_time_this_job_hr,
        circulating_time_this_job_hr,

        -- cost information
        item_cost,
        cost_unit_label,

        -- comments
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
