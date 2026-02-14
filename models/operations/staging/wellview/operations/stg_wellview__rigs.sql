{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'operations']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBRIG') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as job_rig_id,
        trim(idwell)::varchar as well_id,
        trim(idrecparent)::varchar as job_id,
        trim(idrecjobcontactcontractor)::varchar as rig_supervisor_id,
        trim(idrecjobcontactcontractortk)::varchar as rig_supervisor_table_key,

        -- descriptive fields
        trim(proposedoractual)::varchar as proposed_or_actual,
        trim(contractor)::varchar as rig_contractor,
        trim(contractorparent)::varchar as contractor_parent,
        trim(rigno)::varchar as rig_number,
        trim(typ1)::varchar as rig_type,
        trim(typ2)::varchar as rig_subtype,
        trim(contracttyp)::varchar as contract_type,
        trim(category)::varchar as rig_category,
        trim(purpose)::varchar as rig_purpose,
        trim(registration)::varchar as registration_country,
        trim(inventoryno)::varchar as inventory_number,
        trim(dateortyp)::varchar as date_override_type,
        trim(powertyp)::varchar as power_type,
        trim(rotarysystem)::varchar as rotary_system_type,
        trim(derricktyp)::varchar as derrick_type,
        trim(drawworktyp)::varchar as drawworks_type,
        trim(drawworkmake)::varchar as drawworks_manufacturer,
        trim(drawworkmodel)::varchar as drawworks_model,
        trim(postyp)::varchar as positioning_system_type,
        trim(anchortyp)::varchar as anchor_type,
        trim(anchorlinetyp)::varchar as anchor_line_type,
        trim(rigrateref)::varchar as rig_rate_reference,
        trim(com)::varchar as comments,

        -- dates
        dttmstart::timestamp_ntz as rig_start_datetime,
        dttmend::timestamp_ntz as rig_end_datetime,
        dttmstartorcalc::timestamp_ntz as earliest_start_datetime,
        dttmendorcalc::timestamp_ntz as latest_end_datetime,

        -- measurements: duration
        {{ wv_days_to_hours('durcalc') }} as rig_duration_hours,
        {{ wv_days_to_minutes('duronbtmcalc') }} as duration_on_bottom_min,
        {{ wv_days_to_minutes('duroffbtmcalc') }} as duration_off_bottom_min,
        {{ wv_days_to_minutes('durpipemovingcalc') }} as duration_pipe_moving_min,

        -- measurements: depth and height
        {{ wv_meters_to_feet('depthmax') }} as max_rated_depth_ft,
        {{ wv_meters_to_feet('refheight') }} as reference_height_ft,
        {{ wv_meters_to_feet('heightmastclearance') }} as mast_clearance_height_ft,
        {{ wv_meters_to_feet('heightsubclear') }} as sub_clearance_height_ft,
        {{ wv_meters_to_feet('waterdepthmax') }} as max_water_depth_ft,
        {{ wv_meters_to_feet('slipjtextmax') }} as max_slip_joint_extension_ft,

        -- measurements: load and capacity (kN -> klbf: newtons_to_lbf factor * 1000)
        hookloadmax / 4448.2216152605 as max_hook_load_klbf,
        weightblock / 4448.2216152605 as block_weight_klbf,
        {{ wv_kg_to_lb('setbackcapacity') }} as setback_capacity_lb,
        transportloads::float as transport_loads_count,
        maxvariableload / 4448.2216152605 as max_variable_load_klbf,
        anchormaxtension / 4448.2216152605 as max_anchor_tension_klbf,

        -- measurements: torque and power (N-m -> ft-lbf)
        torquemax / 1.3558179483314 as max_torque_ft_lbf,
        {{ wv_watts_to_hp('power') }} as power_rating_hp,

        -- measurements: surface lines
        {{ wv_meters_to_feet('lengthkillline') }} as kill_line_length_ft,
        {{ wv_meters_to_inches('szidkillline') }} as kill_line_id_in,
        {{ wv_cbm_to_bbl('volkilllinecalc') }} as kill_line_volume_bbl,
        {{ wv_meters_to_feet('lengthchokeline') }} as choke_line_length_ft,
        {{ wv_meters_to_inches('szidchokeline') }} as choke_line_id_in,
        {{ wv_cbm_to_bbl('volchokelinecalc') }} as choke_line_volume_bbl,
        {{ wv_cbm_to_bbl('volsurfline') }} as surface_line_volume_bbl,

        -- measurements: offshore
        anchorno::float as anchor_count,

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
        and job_rig_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['job_rig_id']) }} as rig_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        rig_sk,

        -- identifiers
        job_rig_id,
        well_id,
        job_id,
        rig_supervisor_id,
        rig_supervisor_table_key,

        -- descriptive fields
        proposed_or_actual,
        rig_contractor,
        contractor_parent,
        rig_number,
        rig_type,
        rig_subtype,
        contract_type,
        rig_category,
        rig_purpose,
        registration_country,
        inventory_number,
        date_override_type,
        power_type,
        rotary_system_type,
        derrick_type,
        drawworks_type,
        drawworks_manufacturer,
        drawworks_model,
        positioning_system_type,
        anchor_type,
        anchor_line_type,
        rig_rate_reference,
        comments,

        -- dates
        rig_start_datetime,
        rig_end_datetime,
        earliest_start_datetime,
        latest_end_datetime,

        -- measurements: duration
        rig_duration_hours,
        duration_on_bottom_min,
        duration_off_bottom_min,
        duration_pipe_moving_min,

        -- measurements: depth and height
        max_rated_depth_ft,
        reference_height_ft,
        mast_clearance_height_ft,
        sub_clearance_height_ft,
        max_water_depth_ft,
        max_slip_joint_extension_ft,

        -- measurements: load and capacity
        max_hook_load_klbf,
        block_weight_klbf,
        setback_capacity_lb,
        transport_loads_count,
        max_variable_load_klbf,
        max_anchor_tension_klbf,

        -- measurements: torque and power
        max_torque_ft_lbf,
        power_rating_hp,

        -- measurements: surface lines
        kill_line_length_ft,
        kill_line_id_in,
        kill_line_volume_bbl,
        choke_line_length_ft,
        choke_line_id_in,
        choke_line_volume_bbl,
        surface_line_volume_bbl,

        -- measurements: offshore
        anchor_count,

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
