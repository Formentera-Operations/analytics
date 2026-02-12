{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITTANKENTRY') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as tank_entry_id,
        trim(idrecparent)::varchar as tank_id,
        trim(idflownet)::varchar as flow_network_id,

        -- dates
        dttm::timestamp_ntz as reading_date,

        -- reading data
        tankleveltop::float as level_top,
        {{ pv_decimal_to_pct('bsw') }} as bsw_pct,
        {{ pv_decimal_to_pct('sandcut') }} as sand_cut_pct,
        tanklevelfreewater::float as water_bottom,
        tanklevelsand::float as level_sand,

        -- descriptive fields
        trim(com)::varchar as comments,

        -- uncorrected volumes
        {{ pv_cbm_to_bbl('voluncorrtotalcalc') }} as total_volume_uncorrected_bbl,
        {{ pv_cbm_to_bbl('voluncorrhcliqcalc') }} as oil_condensate_volume_uncorrected_bbl,
        {{ pv_cbm_to_bbl('volfreewatercalc') }} as free_water_volume_bbl,
        {{ pv_cbm_to_bbl('volsettledsandcalc') }} as settled_sand_volume_bbl,
        {{ pv_decimal_to_pct('bswuncorrcalc') }} as bsw_uncorrected_pct,
        {{ pv_decimal_to_pct('sandcutuncorrcalc') }} as sand_cut_uncorrected_pct,

        -- temperature and pressure corrections
        tempofvol / 0.555555555555556 + 32 as temperature_of_volume_f,
        {{ pv_kpa_to_psi('presofvol') }} as pressure_of_volume_psi,
        tempsample / 0.555555555555556 + 32 as temperature_of_sample_f,
        {{ pv_kpa_to_psi('pressample') }} as pressure_of_sample_psi,

        -- density measurements
        power(nullif(densitysample, 0), -1) / 7.07409872233005E-06 + -131.5 as density_of_sample_api,
        power(nullif(densitysample60f, 0), -1) / 7.07409872233005E-06 + -131.5 as density_of_sample_60f_api,

        -- volume changes from previous reading
        {{ pv_cbm_to_bbl('volchgtotalcalc') }} as change_in_total_volume_bbl,
        {{ pv_cbm_to_bbl('volchghcliqcalc') }} as change_in_oil_condensate_volume_bbl,
        {{ pv_cbm_to_mcf('volchghcliqgaseqcalc') }} as change_in_gas_equivalent_oil_cond_volume_mcf,
        {{ pv_cbm_to_bbl('volchgwatercalc') }} as change_in_water_volume_bbl,
        {{ pv_cbm_to_bbl('volchgsandcalc') }} as change_in_sand_volume_bbl,

        -- final volumes
        {{ pv_cbm_to_bbl('voltotalcalc') }} as final_total_fluid_volume_bbl,
        {{ pv_cbm_to_bbl('volhcliqcalc') }} as final_hydrocarbon_liquid_volume_bbl,
        {{ pv_cbm_to_mcf('volhcliqgaseqcalc') }} as final_gas_equivalent_oil_condensate_volume_mcf,
        {{ pv_cbm_to_bbl('volwatercalc') }} as final_water_volume_bbl,
        {{ pv_cbm_to_bbl('volsandcalc') }} as final_sand_volume_bbl,

        -- final quality measurements
        {{ pv_decimal_to_pct('bswcalc') }} as final_bsw_pct,
        {{ pv_decimal_to_pct('sandcutcalc') }} as final_sand_cut_pct,

        -- capacity
        {{ pv_cbm_to_bbl('volcapacityremaincalc') }} as capacity_remaining_bbl,

        -- analysis references
        trim(idrechcliqanalysiscalc)::varchar as hc_liquid_analysis_id,
        trim(idrechcliqanalysiscalctk)::varchar as hc_liquid_analysis_table,

        -- user-defined fields
        trim(usertxt1)::varchar as user_text_1,
        trim(usertxt2)::varchar as user_text_2,
        trim(usertxt3)::varchar as user_text_3,
        usernum1::float as user_number_1,
        usernum2::float as user_number_2,
        usernum3::float as user_number_3,
        userdttm1::timestamp_ntz as user_date_1,
        userdttm2::timestamp_ntz as user_date_2,
        userdttm3::timestamp_ntz as load_transfer_request_date,

        -- system / audit
        trim(syscreateuser)::varchar as created_by,
        syscreatedate::timestamp_ntz as created_at_utc,
        trim(sysmoduser)::varchar as modified_by,
        sysmoddate::timestamp_ntz as modified_at_utc,
        trim(systag)::varchar as record_tag,
        syslockdate::timestamp_ntz as lock_date_utc,
        syslockme::boolean as is_locked,
        syslockchildren::boolean as is_children_locked,
        syslockmeui::boolean as is_locked_ui,
        syslockchildrenui::boolean as is_children_locked_ui,

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
        and tank_entry_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['tank_entry_id']) }} as tank_reading_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        tank_reading_sk,

        -- identifiers
        tank_entry_id,
        tank_id,
        flow_network_id,

        -- dates
        reading_date,

        -- reading data
        level_top,
        bsw_pct,
        sand_cut_pct,
        water_bottom,
        level_sand,

        -- descriptive fields
        comments,

        -- uncorrected volumes
        total_volume_uncorrected_bbl,
        oil_condensate_volume_uncorrected_bbl,
        free_water_volume_bbl,
        settled_sand_volume_bbl,
        bsw_uncorrected_pct,
        sand_cut_uncorrected_pct,

        -- temperature and pressure corrections
        temperature_of_volume_f,
        pressure_of_volume_psi,
        temperature_of_sample_f,
        pressure_of_sample_psi,

        -- density measurements
        density_of_sample_api,
        density_of_sample_60f_api,

        -- volume changes from previous reading
        change_in_total_volume_bbl,
        change_in_oil_condensate_volume_bbl,
        change_in_gas_equivalent_oil_cond_volume_mcf,
        change_in_water_volume_bbl,
        change_in_sand_volume_bbl,

        -- final volumes
        final_total_fluid_volume_bbl,
        final_hydrocarbon_liquid_volume_bbl,
        final_gas_equivalent_oil_condensate_volume_mcf,
        final_water_volume_bbl,
        final_sand_volume_bbl,

        -- final quality measurements
        final_bsw_pct,
        final_sand_cut_pct,

        -- capacity
        capacity_remaining_bbl,

        -- analysis references
        hc_liquid_analysis_id,
        hc_liquid_analysis_table,

        -- user-defined fields
        user_text_1,
        user_text_2,
        user_text_3,
        user_number_1,
        user_number_2,
        user_number_3,
        user_date_1,
        user_date_2,
        load_transfer_request_date,

        -- system / audit
        created_by,
        created_at_utc,
        modified_by,
        modified_at_utc,
        record_tag,
        lock_date_utc,
        is_locked,
        is_children_locked,
        is_locked_ui,
        is_children_locked_ui,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
