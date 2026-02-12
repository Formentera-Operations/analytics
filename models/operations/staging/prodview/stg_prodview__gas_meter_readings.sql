{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITMETERORIFICEENTRY') }}
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
        trim(idrecgasanalysiscalc)::varchar as gas_analysis_id,
        trim(idrecgasanalysiscalctk)::varchar as gas_analysis_table,

        -- dates
        dttm::timestamp_ntz as reading_date,

        -- duration
        {{ pv_days_to_hours('duronor') }}::float as override_duration_hours,
        {{ pv_days_to_hours('duroncalc') }}::float as calculated_duration_hours,

        -- raw pressure and temperature (unconverted source values)
        presstatic::float as static_pressure_raw,
        presdiff::float as differential_pressure_raw,
        temp::float as temperature_raw,

        -- calculated pressure and temperature
        {{ pv_kpa_to_psi('presstaticcalc') }}::float as calculated_static_pressure_psi,
        {{ pv_kpa_to_psi('presdiffcalc') }}::float as calculated_differential_pressure_psi,
        tempcalc / 0.555555555555556 + 32 as calculated_temperature_f,

        -- orifice data
        cprime::float as c_prime_factor,
        szorifice::float as orifice_size,

        -- gas volumes
        {{ pv_cbm_to_mcf('voluncorrgascalc') }}::float as uncorrected_gas_volume_mcf,
        {{ pv_cbm_to_mcf('volentergas') }}::float as entered_gas_volume_mcf,
        {{ pv_cbm_to_mcf('volenterorgas') }}::float as override_gas_volume_mcf,
        {{ pv_cbm_to_mcf('volgascalc') }}::float as calculated_gas_volume_mcf,
        volsourcecalc::float as source_volume_calculation,
        trim(reasonor)::varchar as override_reason,

        -- heat values
        {{ pv_joules_to_mmbtu('heatenter') }}::float as entered_heat_mmbtu,
        {{ pv_jm3_to_btu_per_ft3('factheatenter') }}::float as entered_heat_factor_btu_per_ft3,
        {{ pv_joules_to_mmbtu('heatenteror') }}::float as override_heat_mmbtu,
        {{ pv_jm3_to_btu_per_ft3('factheatenteror') }}::float as override_heat_factor_btu_per_ft3,
        {{ pv_joules_to_mmbtu('heatcalc') }}::float as calculated_heat_mmbtu,
        {{ pv_jm3_to_btu_per_ft3('factheatcalc') }}::float as calculated_heat_factor_btu_per_ft3,

        -- regulatory codes
        trim(regulatorycode1)::varchar as regulatory_code_1,
        trim(regulatorycode2)::varchar as regulatory_code_2,
        trim(regulatorycode3)::varchar as regulatory_code_3,

        -- comments
        trim(com)::varchar as comments,
        trim(comffv)::varchar as ffv_comments,

        -- user-defined fields
        trim(usertxt1)::varchar as user_text_1,
        trim(usertxt2)::varchar as user_text_2,
        trim(usertxt3)::varchar as user_text_3,
        usernum1::float as user_number_1,
        usernum2::float as user_number_2,
        usernum3::float as user_number_3,
        userdttm1::timestamp_ntz as user_date_1,
        userdttm2::timestamp_ntz as user_date_2,
        userdttm3::timestamp_ntz as user_date_3,

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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as gas_meter_reading_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        gas_meter_reading_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,
        gas_analysis_id,
        gas_analysis_table,

        -- dates
        reading_date,

        -- duration
        override_duration_hours,
        calculated_duration_hours,

        -- raw pressure and temperature
        static_pressure_raw,
        differential_pressure_raw,
        temperature_raw,

        -- calculated pressure and temperature
        calculated_static_pressure_psi,
        calculated_differential_pressure_psi,
        calculated_temperature_f,

        -- orifice data
        c_prime_factor,
        orifice_size,

        -- gas volumes
        uncorrected_gas_volume_mcf,
        entered_gas_volume_mcf,
        override_gas_volume_mcf,
        calculated_gas_volume_mcf,
        source_volume_calculation,
        override_reason,

        -- heat values
        entered_heat_mmbtu,
        entered_heat_factor_btu_per_ft3,
        override_heat_mmbtu,
        override_heat_factor_btu_per_ft3,
        calculated_heat_mmbtu,
        calculated_heat_factor_btu_per_ft3,

        -- regulatory codes
        regulatory_code_1,
        regulatory_code_2,
        regulatory_code_3,

        -- comments
        comments,
        ffv_comments,

        -- user-defined fields
        user_text_1,
        user_text_2,
        user_text_3,
        user_number_1,
        user_number_2,
        user_number_3,
        user_date_1,
        user_date_2,
        user_date_3,

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
