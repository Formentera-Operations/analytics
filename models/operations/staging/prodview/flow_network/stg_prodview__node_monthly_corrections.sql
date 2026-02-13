{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITNODECORR') }}
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

        -- correction period
        dttmstart::timestamp_ntz as correction_start_date,
        dttmend::timestamp_ntz as correction_end_date,

        -- reference information
        trim(refid)::varchar as reference_number,
        trim(origstatementid)::varchar as statement_id,
        trim(source)::varchar as data_source,

        -- temperature measurements (converted to Fahrenheit)
        temp / 0.555555555555556 + 32 as temperature_f,
        tempsample / 0.555555555555556 + 32 as temperature_of_sample_f,

        -- pressure measurements (converted to PSI)
        {{ pv_kpa_to_psi('pres') }}::float as pressure_psi,
        {{ pv_kpa_to_psi('pressample') }}::float as pressure_of_sample_psi,

        -- density measurements (converted to API gravity)
        power(nullif(density, 0), -1) / 7.07409872233005e-06 + -131.5 as density_api,
        power(nullif(density60f, 0), -1) / 7.07409872233005e-06 + -131.5 as density_at_60f_api,
        power(nullif(densitysample, 0), -1) / 7.07409872233005e-06 + -131.5 as density_of_sample_api,

        -- final corrected volumes (converted to US units)
        {{ pv_cbm_to_bbl('volhcliq') }}::float as final_corrected_hcliq_bbl,
        {{ pv_cbm_to_mcf('volgas') }}::float as final_gas_mcf,
        {{ pv_cbm_to_bbl('volwater') }}::float as final_water_bbl,
        {{ pv_cbm_to_bbl('volsand') }}::float as final_sand_bbl,

        -- heat values (converted to US units)
        {{ pv_joules_to_mmbtu('heat') }}::float as final_corrected_heat_mmbtu,
        {{ pv_jm3_to_btu_per_ft3('factheat') }}::float as final_corrected_heat_factor_btu_per_ft3,

        -- heat estimates and differences
        {{ pv_joules_to_mmbtu('estheat') }}::float as estimated_heat_mmbtu,
        {{ pv_joules_to_mmbtu('diffheat') }}::float as heat_difference_mmbtu,
        {{ pv_decimal_to_pct('pctdiffheat') }}::float as heat_correction_pct,

        -- gas estimates and differences (converted to MCF)
        {{ pv_cbm_to_mcf('volestgas') }}::float as estimated_gas_volume_mcf,
        {{ pv_cbm_to_mcf('voldiffgas') }}::float as volume_difference_gas_mcf,
        {{ pv_decimal_to_pct('pctdiffgas') }}::float as gas_correction_pct,

        -- hcliq estimates and differences (converted to barrels)
        {{ pv_cbm_to_bbl('volesthcliq') }}::float as estimated_volume_hcliq_bbl,
        {{ pv_cbm_to_bbl('voldiffhcliq') }}::float as volume_difference_hcliq_bbl,
        {{ pv_decimal_to_pct('pctdiffhcliq') }}::float as hcliq_correction_pct,

        -- water estimates and differences (converted to barrels)
        {{ pv_cbm_to_bbl('volestwater') }}::float as estimated_volume_water_bbl,
        {{ pv_cbm_to_bbl('voldiffwater') }}::float as volume_difference_water_bbl,
        {{ pv_decimal_to_pct('pctdiffwater') }}::float as water_correction_pct,

        -- sand estimates and differences (converted to barrels)
        {{ pv_cbm_to_bbl('volestsand') }}::float as estimated_volume_sand_bbl,
        {{ pv_cbm_to_bbl('voldiffsand') }}::float as volume_difference_sand_bbl,
        {{ pv_decimal_to_pct('pctdiffsand') }}::float as sand_correction_pct,

        -- duration
        duractual::float as actual_meter_time_days,

        -- general information
        trim(com)::varchar as comments,

        -- user-defined fields
        trim(usertxt1)::varchar as user_txt1,
        trim(usertxt2)::varchar as user_txt2,
        trim(usertxt3)::varchar as user_txt3,
        trim(usertxt4)::varchar as user_txt4,
        trim(usertxt5)::varchar as user_txt5,
        usernum1::float as user_num1,
        usernum2::float as user_num2,
        usernum3::float as user_num3,
        usernum4::float as user_num4,
        usernum5 / 0.555555555555556 + 32 as user_num5_temperature_f,

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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as node_monthly_correction_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        node_monthly_correction_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,

        -- correction period
        correction_start_date,
        correction_end_date,

        -- reference information
        reference_number,
        statement_id,
        data_source,

        -- temperature measurements
        temperature_f,
        temperature_of_sample_f,

        -- pressure measurements
        pressure_psi,
        pressure_of_sample_psi,

        -- density measurements
        density_api,
        density_at_60f_api,
        density_of_sample_api,

        -- final corrected volumes
        final_corrected_hcliq_bbl,
        final_gas_mcf,
        final_water_bbl,
        final_sand_bbl,

        -- heat values
        final_corrected_heat_mmbtu,
        final_corrected_heat_factor_btu_per_ft3,

        -- heat estimates and differences
        estimated_heat_mmbtu,
        heat_difference_mmbtu,
        heat_correction_pct,

        -- gas estimates and differences
        estimated_gas_volume_mcf,
        volume_difference_gas_mcf,
        gas_correction_pct,

        -- hcliq estimates and differences
        estimated_volume_hcliq_bbl,
        volume_difference_hcliq_bbl,
        hcliq_correction_pct,

        -- water estimates and differences
        estimated_volume_water_bbl,
        volume_difference_water_bbl,
        water_correction_pct,

        -- sand estimates and differences
        estimated_volume_sand_bbl,
        volume_difference_sand_bbl,
        sand_correction_pct,

        -- duration
        actual_meter_time_days,

        -- general information
        comments,

        -- user-defined fields
        user_txt1,
        user_txt2,
        user_txt3,
        user_txt4,
        user_txt5,
        user_num1,
        user_num2,
        user_num3,
        user_num4,
        user_num5_temperature_f,

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
