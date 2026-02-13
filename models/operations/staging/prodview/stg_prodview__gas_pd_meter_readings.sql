{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITMETERPDGASENTRY') }}
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

        -- date/time and readings
        dttm::timestamp_ntz as reading_date,
        readingend::float as reading_value,

        -- temperature and pressure (converted to US units)
        temp / 0.555555555555556 + 32 as temperature_f,
        {{ pv_kpa_to_psi('pres') }}::float as pressure_psi,

        -- calculated volume (converted to MCF)
        {{ pv_cbm_to_mcf('volgascalc') }}::float as calculated_gas_volume_mcf,

        -- override values
        readingendor::float as reading_override,
        trim(readingendorreason)::varchar as reading_override_reason,
        readingstartor::float as start_reading_override,
        trim(reasonor)::varchar as start_override_reason,

        -- heat content (converted to US units)
        {{ pv_joules_to_mmbtu('heat') }}::float as heat_mmbtu,
        {{ pv_jm3_to_btu_per_ft3('factheat') }}::float as heat_factor_btu_per_ft3,

        -- regulatory codes
        trim(regulatorycode1)::varchar as regulatory_code_1,
        trim(regulatorycode2)::varchar as regulatory_code_2,
        trim(regulatorycode3)::varchar as regulatory_code_3,

        -- comments
        trim(com)::varchar as note,
        trim(comffv)::varchar as ffv_note,

        -- gas analysis reference
        trim(idrecgasanalysiscalc)::varchar as gas_analysis_id,
        trim(idrecgasanalysiscalctk)::varchar as gas_analysis_table,

        -- user-defined fields
        trim(usertxt1)::varchar as user_txt1,
        trim(usertxt2)::varchar as user_txt2,
        trim(usertxt3)::varchar as user_txt3,
        usernum1::float as user_num1,
        usernum2::float as user_num2,
        usernum3::float as user_num3,
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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as gas_pd_meter_reading_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        gas_pd_meter_reading_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,

        -- date/time and readings
        reading_date,
        reading_value,

        -- temperature and pressure
        temperature_f,
        pressure_psi,

        -- calculated volume
        calculated_gas_volume_mcf,

        -- override values
        reading_override,
        reading_override_reason,
        start_reading_override,
        start_override_reason,

        -- heat content
        heat_mmbtu,
        heat_factor_btu_per_ft3,

        -- regulatory codes
        regulatory_code_1,
        regulatory_code_2,
        regulatory_code_3,

        -- comments
        note,
        ffv_note,

        -- gas analysis reference
        gas_analysis_id,
        gas_analysis_table,

        -- user-defined fields
        user_txt1,
        user_txt2,
        user_txt3,
        user_num1,
        user_num2,
        user_num3,
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
