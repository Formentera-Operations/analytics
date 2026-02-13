{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPPARAM') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as id_rec,
        trim(idrecparent)::varchar as id_rec_parent,
        trim(idflownet)::varchar as id_flownet,

        -- measurement date
        dttm::timestamp_ntz as measurement_date,

        -- pressure measurements (kPa → PSI)
        ({{ pv_kpa_to_psi('prestub') }})::float as tubing_pressure_psi,
        ({{ pv_kpa_to_psi('prescas') }})::float as casing_pressure_psi,
        ({{ pv_kpa_to_psi('presannulus') }})::float as annulus_pressure_psi,
        ({{ pv_kpa_to_psi('presline') }})::float as line_pressure_psi,
        ({{ pv_kpa_to_psi('presinj') }})::float as injection_pressure_psi,
        ({{ pv_kpa_to_psi('preswh') }})::float as wellhead_pressure_psi,
        ({{ pv_kpa_to_psi('presbh') }})::float as bottomhole_pressure_psi,
        ({{ pv_kpa_to_psi('prestubsi') }})::float as shut_in_tubing_pressure_psi,
        ({{ pv_kpa_to_psi('prescassi') }})::float as shut_in_casing_pressure_psi,

        -- temperature measurements (Celsius → Fahrenheit, no macro available)
        (tempwh / 0.555555555555556 + 32)::float as wellhead_temp_f,
        (tempbh / 0.555555555555556 + 32)::float as bottomhole_temp_f,

        -- equipment specifications
        ({{ pv_meters_to_64ths_inch('szchoke') }})::float as choke_size_64ths,

        -- fluid properties
        viscdynamic::float as dynamic_viscosity_pa_s,
        ({{ pv_m2s_to_in2s('visckinematic') }})::float as kinematic_viscosity_in2_per_s,
        ph::float as ph_level,
        (salinity / 1e-06)::float as h2s_daily_reading_ppm,

        -- user-defined pressure measurements (kPa → PSI)
        ({{ pv_kpa_to_psi('presuser1') }})::float as surface_casing_pressure_psi,
        ({{ pv_kpa_to_psi('presuser2') }})::float as intermediate_casing_pressure_psi,
        ({{ pv_kpa_to_psi('presuser3') }})::float as plunger_on_pressure_psi,
        ({{ pv_kpa_to_psi('presuser4') }})::float as user_pressure_4_psi,
        ({{ pv_kpa_to_psi('presuser5') }})::float as annulus_pressure_2_psi,

        -- user-defined temperature measurements (Celsius → Fahrenheit)
        (tempuser1 / 0.555555555555556 + 32)::float as treater_temp_f,
        (tempuser2 / 0.555555555555556 + 32)::float as user_temp_2_f,
        (tempuser3 / 0.555555555555556 + 32)::float as user_temp_3_f,
        (tempuser4 / 0.555555555555556 + 32)::float as fluid_level_csg_pressure_f,
        (tempuser5 / 0.555555555555556 + 32)::float as fluid_level_tbg_pressure_f,

        -- user-defined fields - text (plunger information)
        trim(usertxt1)::varchar as spcc_inspection_complete,
        trim(usertxt2)::varchar as plunger_model,
        trim(usertxt3)::varchar as plunger_make,
        trim(usertxt4)::varchar as plunger_size,
        trim(usertxt5)::varchar as operational_work,

        -- user-defined fields - numeric (plunger operations)
        usernum1::float as cycles,
        usernum2::float as arrivals,
        ({{ pv_seconds_to_minutes('usernum3') }})::float as travel_time_min,
        ({{ pv_seconds_to_minutes('usernum4') }})::float as after_flow_min,
        ({{ pv_seconds_to_minutes('usernum5') }})::float as shut_in_time_min,

        -- user-defined fields - datetime
        userdttm1::timestamp_ntz as plunger_inspection_date,
        userdttm2::timestamp_ntz as plunger_replace_date,
        userdttm3::timestamp_ntz as user_date_3,
        userdttm4::timestamp_ntz as user_date_4,
        userdttm5::timestamp_ntz as user_date_5,

        -- notes
        trim(com)::varchar as notes,

        -- system / audit
        trim(syscreateuser)::varchar as created_by,
        syscreatedate::timestamp_ntz as created_at_utc,
        trim(sysmoduser)::varchar as modified_by,
        sysmoddate::timestamp_ntz as modified_at_utc,
        syslockdate::timestamp_ntz as lock_date_utc,
        syslockme::boolean as is_locked,
        syslockchildren::boolean as is_children_locked,
        syslockmeui::boolean as is_locked_ui,
        syslockchildrenui::boolean as is_children_locked_ui,
        trim(systag)::varchar as record_tag,

        -- fivetran metadata
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_tz as _fivetran_synced

    from source
),

filtered as (
    select *
    from renamed
    where
        coalesce(_fivetran_deleted, false) = false
        and id_rec is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as completion_parameter_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        completion_parameter_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,

        -- measurement date
        measurement_date,

        -- pressure measurements
        tubing_pressure_psi,
        casing_pressure_psi,
        annulus_pressure_psi,
        line_pressure_psi,
        injection_pressure_psi,
        wellhead_pressure_psi,
        bottomhole_pressure_psi,
        shut_in_tubing_pressure_psi,
        shut_in_casing_pressure_psi,

        -- temperature measurements
        wellhead_temp_f,
        bottomhole_temp_f,

        -- equipment specifications
        choke_size_64ths,

        -- fluid properties
        dynamic_viscosity_pa_s,
        kinematic_viscosity_in2_per_s,
        ph_level,
        h2s_daily_reading_ppm,

        -- user-defined pressure measurements
        surface_casing_pressure_psi,
        intermediate_casing_pressure_psi,
        plunger_on_pressure_psi,
        user_pressure_4_psi,
        annulus_pressure_2_psi,

        -- user-defined temperature measurements
        treater_temp_f,
        user_temp_2_f,
        user_temp_3_f,
        fluid_level_csg_pressure_f,
        fluid_level_tbg_pressure_f,

        -- user-defined fields - text
        spcc_inspection_complete,
        plunger_model,
        plunger_make,
        plunger_size,
        operational_work,

        -- user-defined fields - numeric
        cycles,
        arrivals,
        travel_time_min,
        after_flow_min,
        shut_in_time_min,

        -- user-defined fields - datetime
        plunger_inspection_date,
        plunger_replace_date,
        user_date_3,
        user_date_4,
        user_date_5,

        -- notes
        notes,

        -- system / audit
        created_by,
        created_at_utc,
        modified_by,
        modified_at_utc,
        lock_date_utc,
        is_locked,
        is_children_locked,
        is_locked_ui,
        is_children_locked_ui,
        record_tag,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
