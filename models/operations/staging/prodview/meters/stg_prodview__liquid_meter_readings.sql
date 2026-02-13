{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITMETERLIQUIDENTRY') }}
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
        trim(idrechcliqanalysiscalc)::varchar as hc_liquid_analysis_id,
        trim(idrechcliqanalysiscalctk)::varchar as hc_liquid_analysis_table,
        trim(idrecsealentry)::varchar as seal_entry_id,
        trim(idrecsealentrytk)::varchar as seal_entry_table,

        -- dates
        dttm::timestamp_ntz as reading_date,

        -- meter readings
        readingstart::float as reading_start,
        readingend::float as reading_end,

        -- raw measurements (percentages)
        {{ pv_decimal_to_pct('bsw') }}::float as basic_sediment_water_pct,
        {{ pv_decimal_to_pct('sandcut') }}::float as sand_cut_pct,

        -- uncorrected volumes
        {{ pv_cbm_to_bbl('voluncorrtotalcalc') }}::float as uncorrected_total_volume_bbl,
        {{ pv_cbm_to_bbl('voluncorrhcliqcalc') }}::float as uncorrected_hcliq_volume_bbl,

        -- temperature and pressure of volume
        tempofvol / 0.555555555555556 + 32 as volume_temperature_f,
        {{ pv_kpa_to_psi('presofvol') }}::float as volume_pressure_psi,

        -- sample measurements
        tempsample / 0.555555555555556 + 32 as sample_temperature_f,
        {{ pv_kpa_to_psi('pressample') }}::float as sample_pressure_psi,
        power(nullif(densitysample, 0), -1) / 7.07409872233005E-06 + -131.5 as sample_density_api,
        power(nullif(densitysample60f, 0), -1) / 7.07409872233005E-06 + -131.5 as sample_density_60f_api,

        -- corrected volumes and percentages
        {{ pv_cbm_to_bbl('volcorrtotalcalc') }}::float as corrected_total_volume_bbl,
        {{ pv_cbm_to_bbl('volcorrhcliqcalc') }}::float as corrected_hcliq_volume_bbl,
        {{ pv_decimal_to_pct('bswcorrcalc') }}::float as corrected_bsw_pct,
        {{ pv_decimal_to_pct('sandcutcorrcalc') }}::float as corrected_sand_cut_pct,

        -- override values
        tempor / 0.555555555555556 + 32 as override_temperature_f,
        {{ pv_kpa_to_psi('presor') }}::float as override_pressure_psi,
        power(nullif(densityor, 0), -1) / 7.07409872233005E-06 + -131.5 as override_density_api,
        trim(reasonor)::varchar as override_reason,
        {{ pv_cbm_to_bbl('volorhcliq') }}::float as override_hcliq_volume_bbl,
        {{ pv_cbm_to_bbl('volorwater') }}::float as override_water_volume_bbl,
        {{ pv_cbm_to_bbl('volorsand') }}::float as override_sand_volume_bbl,

        -- final calculated volumes
        {{ pv_cbm_to_bbl('voltotalcalc') }}::float as total_volume_bbl,
        {{ pv_cbm_to_bbl('volhcliqcalc') }}::float as hcliq_volume_bbl,
        {{ pv_cbm_to_mcf('volhcliqgaseqcalc') }}::float as hcliq_gas_equivalent_mcf,
        {{ pv_cbm_to_bbl('volwatercalc') }}::float as water_volume_bbl,
        {{ pv_cbm_to_bbl('volsandcalc') }}::float as sand_volume_bbl,
        {{ pv_decimal_to_pct('bswcalc') }}::float as final_bsw_pct,
        {{ pv_decimal_to_pct('sandcutcalc') }}::float as final_sand_cut_pct,

        -- reference data
        trim(ticketno)::varchar as ticket_number,
        trim(ticketsubno)::varchar as ticket_sub_number,
        trim(refid)::varchar as reference_id,
        trim(origstatementid)::varchar as original_statement_id,
        trim(source)::varchar as data_source,
        verified::boolean as is_verified,
        trim(com)::varchar as comments,

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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as liquid_meter_reading_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        liquid_meter_reading_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,
        hc_liquid_analysis_id,
        hc_liquid_analysis_table,
        seal_entry_id,
        seal_entry_table,

        -- dates
        reading_date,

        -- meter readings
        reading_start,
        reading_end,

        -- raw measurements
        basic_sediment_water_pct,
        sand_cut_pct,

        -- uncorrected volumes
        uncorrected_total_volume_bbl,
        uncorrected_hcliq_volume_bbl,

        -- temperature and pressure of volume
        volume_temperature_f,
        volume_pressure_psi,

        -- sample measurements
        sample_temperature_f,
        sample_pressure_psi,
        sample_density_api,
        sample_density_60f_api,

        -- corrected volumes and percentages
        corrected_total_volume_bbl,
        corrected_hcliq_volume_bbl,
        corrected_bsw_pct,
        corrected_sand_cut_pct,

        -- override values
        override_temperature_f,
        override_pressure_psi,
        override_density_api,
        override_reason,
        override_hcliq_volume_bbl,
        override_water_volume_bbl,
        override_sand_volume_bbl,

        -- final calculated volumes
        total_volume_bbl,
        hcliq_volume_bbl,
        hcliq_gas_equivalent_mcf,
        water_volume_bbl,
        sand_volume_bbl,
        final_bsw_pct,
        final_sand_cut_pct,

        -- reference data
        ticket_number,
        ticket_sub_number,
        reference_id,
        original_statement_id,
        data_source,
        is_verified,
        comments,

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
