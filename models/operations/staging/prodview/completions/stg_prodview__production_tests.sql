{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPTEST') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as id_rec,
        trim(idrecparent)::varchar as id_rec_parent, -- completion_id
        trim(idflownet)::varchar as id_flownet,

        -- test information
        trim(typ)::varchar as test_type,
        dttm::timestamp_ntz as effective_date,
        {{ pv_days_to_hours('durtest') }}::float as test_hours,
        trim(dontuse)::varchar as invalid_test,
        trim(testedby)::varchar as tested_by,
        trim(com)::varchar as note,
        trailerduration::float as trailer_duration_days,

        -- separator reference
        trim(idrecsep)::varchar as separator_vessel_id,
        trim(idrecseptk)::varchar as separator_vessel_table,
        trim(idrecsepunitcalc)::varchar as separator_vessel_unit_calc_id,
        trim(idrecsepunitcalctk)::varchar as separator_vessel_unit_calc_table,

        -- equipment conditions
        {{ pv_meters_to_64ths_inch('szchoke') }}::float as choke_size_64ths,

        -- gas measurements
        cprime::float as c_prime,
        {{ pv_days_to_hours('duronor') }}::float as hours_on_override,
        presgasstatic::float as orifice_static_pressure_gauge,
        presgasdiff::float as orifice_differential_pressure,
        tempgas::float as orifice_temperature,
        szorifice::float as orifice_size,
        {{ pv_cbm_to_mcf('volentergas') }}::float as gas_volume_entry_mcf,
        {{ pv_cbm_to_mcf('volgas') }}::float as gas_volume_override_mcf,
        {{ pv_cbm_to_mcf('volgascalc') }}::float as test_gas_volume_mcf,

        -- gas lift
        {{ pv_cbm_to_mcf('volliftgasrecov') }}::float as recovered_lift_gas_mcf,
        {{ pv_cbm_to_mcf('ratetotalgascalc') }}::float as total_gas_rate_mcf_per_day,
        {{ pv_cbm_to_mcf('rateliftgascalc') }}::float as lift_gas_rate_mcf_per_day,

        -- oil emulsion measurements
        readinghcliqstart::float as start_reading_oil_emulsion,
        readinghcliqend::float as end_reading_oil_emulsion,
        leveltankstart::float as tank_start_level,
        leveltankend::float as tank_end_level,
        leveltankfreewaterstart::float as tank_free_water_start_level,
        leveltankfreewaterend::float as tank_free_water_end_level,
        {{ pv_cbm_to_bbl('voltankstartcalc') }}::float as tank_start_volume_bbl,
        {{ pv_cbm_to_bbl('voltankendcalc') }}::float as tank_end_volume_bbl,
        {{ pv_decimal_to_pct('bsw') }}::float as oil_emul_bsw_pct,
        {{ pv_decimal_to_pct('sandcut') }}::float as oil_emul_sand_cut_pct,
        {{ pv_cbm_ratio_to_mcf_per_bbl('factgasinsoln') }}::float as gas_in_solution_factor_mcf_per_bbl,
        {{ pv_cbm_to_bbl('volenterhcliq') }}::float as oil_emulsion_volume_entry_bbl,
        {{ pv_cbm_to_bbl('volhcliq') }}::float as oil_emulsion_volume_override_bbl,
        {{ pv_cbm_to_mcf('volhcliqgaseqcalc') }}::float as gas_equivalent_of_liquids_volume_mcf,
        {{ pv_cbm_to_bbl('volfluidtotalcalc') }}::float as test_total_fluid_volume_bbl,
        {{ pv_cbm_to_bbl('volhcliqcalc') }}::float as test_oil_condensate_volume_bbl,
        {{ pv_cbm_to_bbl('volbeforetpcorrhcliqcalc') }}::float as oil_cond_vol_before_tp_correction_bbl,

        -- water measurements
        readingwaterstart::float as water_start_reading,
        readingwaterend::float as water_end_reading,
        {{ pv_cbm_to_bbl('volenterwater') }}::float as water_volume_entry_bbl,
        {{ pv_cbm_to_bbl('volwater') }}::float as water_volume_override_bbl,
        {{ pv_cbm_to_bbl('volwatercalc') }}::float as test_water_volume_bbl,

        -- sand measurements
        {{ pv_cbm_to_bbl('volentersand') }}::float as sand_volume_entry_bbl,
        {{ pv_cbm_to_bbl('volsand') }}::float as sand_volume_override_bbl,
        {{ pv_cbm_to_bbl('volsandcalc') }}::float as test_sand_volume_bbl,

        -- test results - rates
        {{ pv_cbm_to_mcf('rateprodgascalc') }}::float as produced_gas_rate_mcf_per_day,
        {{ pv_cbm_to_bbl_per_day('ratefluidtotalcalc') }}::float as total_fluid_rate_bbl_per_day,
        {{ pv_cbm_to_bbl_per_day('ratehcliqcalc') }}::float as oil_cond_rate_bbl_per_day,
        {{ pv_cbm_to_mcf('ratehcliqgaseqcalc') }}::float as gas_equiv_of_oil_cond_rate_mcf_per_day,
        {{ pv_cbm_to_bbl_per_day('ratewatercalc') }}::float as water_rate_bbl_per_day,
        {{ pv_cbm_to_bbl_per_day('ratesandcalc') }}::float as sand_rate_bbl_per_day,

        -- test results - quality metrics
        {{ pv_decimal_to_pct('bswtotalcalc') }}::float as total_bsw_pct,
        {{ pv_decimal_to_pct('sandcuttotalcalc') }}::float as total_sand_cut_pct,
        {{ pv_cbm_ratio_to_mcf_per_bbl('gorcalc') }}::float as gas_oil_ratio_mcf_per_bbl,
        cgrcalc / 0.00561458333333333 as condensate_gas_ratio_bbl_per_mcf,
        wgrcalc / 0.00561458333333333 as water_gas_ratio_bbl_per_mcf,

        -- test conditions - pressures
        {{ pv_kpa_to_psi('presbh') }}::float as pressure_bh_psi,
        {{ pv_kpa_to_psi('prescas') }}::float as casing_pressure_psi,
        {{ pv_kpa_to_psi('presinjectgasliftgas') }}::float as pressure_inject_gas_lift_gas_psi,
        {{ pv_kpa_to_psi('presprodsep') }}::float as pressure_prod_sep_psi,
        {{ pv_kpa_to_psi('prestestsep') }}::float as pressure_test_separator_psi,
        {{ pv_kpa_to_psi('preswh') }}::float as pressure_wellhead_psi,

        -- test conditions - temperatures (no macro, keep inline)
        tempbh / 0.555555555555556 + 32 as temperature_bottom_hole_f,
        tempprodsep / 0.555555555555556 + 32 as temperature_production_separator_f,
        temptestsep / 0.555555555555556 + 32 as temperature_test_separator_f,
        tempwh / 0.555555555555556 + 32 as temperature_well_head_f,

        -- previous test comparisons - rate changes
        {{ pv_cbm_to_bbl_per_day('ratechghcliqcalc') }}::float as change_in_oil_emulsion_rate_bbl_per_day,
        {{ pv_decimal_to_pct('pctchghcliqcalc') }}::float as pct_change_in_oil_emulsion_rate_pct,
        {{ pv_cbm_to_mcf('ratechggascalc') }}::float as change_in_gas_rate_mcf_per_day,
        {{ pv_decimal_to_pct('pctchggascalc') }}::float as pct_change_in_gas_rate_pct,
        {{ pv_cbm_to_bbl_per_day('ratechgwatercalc') }}::float as change_in_water_rate_bbl_per_day,
        {{ pv_decimal_to_pct('pctchgwatercalc') }}::float as pct_change_in_water_rate_pct,
        {{ pv_cbm_to_bbl_per_day('ratechgsandcalc') }}::float as change_in_sand_rate_bbl_per_day,
        {{ pv_decimal_to_pct('pctchgsandcalc') }}::float as pct_change_in_sand_rate_pct,

        -- previous test comparisons - ratio changes
        {{ pv_cbm_ratio_to_mcf_per_bbl('chggorcalc') }}::float as change_in_gor_mcf_per_bbl,
        {{ pv_decimal_to_pct('pctchggorcalc') }}::float as pct_change_in_gor_pct,
        chgcgrcalc / 0.00561458333333333 as change_in_cgr_bbl_per_mcf,
        {{ pv_decimal_to_pct('pctchgcgrcalc') }}::float as pct_change_in_cgr_pct,
        chgwgrcalc / 0.00561458333333333 as change_in_wgr_bbl_per_mcf,
        {{ pv_decimal_to_pct('pctchgwgrcalc') }}::float as pct_change_in_wgr_pct,
        {{ pv_decimal_to_pct('chgbswcalc') }}::float as change_in_bsw_pct,
        {{ pv_decimal_to_pct('pctchgbswcalc') }}::float as pct_change_in_bsw_pct,
        {{ pv_decimal_to_pct('chgsandcutcalc') }}::float as change_in_sand_cut_pct,
        {{ pv_decimal_to_pct('pctchgsandcutcalc') }}::float as pct_change_in_sand_cut_pct,
        trim(reasonvariance)::varchar as reason_for_variance,

        -- reference information
        trim(datasource)::varchar as data_source,
        trim(purposealloc)::varchar as allocation_flag,
        trim(purposedeliv)::varchar as deliverability_flag,
        trim(purposereg)::varchar as regulatory_flag,

        -- temperature and pressure correction
        tempstart / 0.555555555555556 + 32 as temperature_of_opening_level_f,
        {{ pv_kpa_to_psi('presstart') }}::float as pressure_of_opening_level_psi,
        tempend / 0.555555555555556 + 32 as temperature_of_closing_level_f,
        {{ pv_kpa_to_psi('presend') }}::float as pressure_of_closing_level_psi,
        tempsample / 0.555555555555556 + 32 as temperature_of_sample_f,
        {{ pv_kpa_to_psi('pressample') }}::float as pressure_of_sample_psi,
        power(nullif(densitysample, 0), -1) / 7.07409872233005E-06 + -131.5 as density_of_sample_api,
        power(nullif(densitysample60f, 0), -1) / 7.07409872233005E-06 + -131.5 as density_of_sample_at_60f_api,

        -- regulatory properties
        {{ pv_kpa_to_psi('preswhsi') }}::float as shut_in_wellhead_pressure_psi,
        {{ pv_kpa_to_psi('preswhflow') }}::float as flowing_wellhead_pressure_psi,
        {{ pv_kpa_to_psi('presbradenhead') }}::float as bradenhead_pressure_psi,
        trim(reasonbradenhead)::varchar as bradenhead_reason,
        densityrelgas::float as gas_specific_gravity,
        power(nullif(densitycond, 0), -1) / 7.07409872233005E-06 + -131.5 as condensate_gravity_api,

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
        usernum5::float as user_num5,
        userdttm1::timestamp_ntz as user_date_1,
        userdttm2::timestamp_ntz as user_date_2,
        userdttm3::timestamp_ntz as user_date_3,
        userdttm4::timestamp_ntz as user_date_4,
        userdttm5::timestamp_ntz as user_date_5,

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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as production_test_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        production_test_sk,

        -- identifiers
        id_rec,
        id_rec_parent, -- completion_id
        id_flownet,

        -- test information
        test_type,
        effective_date,
        test_hours,
        invalid_test,
        tested_by,
        note,
        trailer_duration_days,

        -- separator reference
        separator_vessel_id,
        separator_vessel_table,
        separator_vessel_unit_calc_id,
        separator_vessel_unit_calc_table,

        -- equipment conditions
        choke_size_64ths,

        -- gas measurements
        c_prime,
        hours_on_override,
        orifice_static_pressure_gauge,
        orifice_differential_pressure,
        orifice_temperature,
        orifice_size,
        gas_volume_entry_mcf,
        gas_volume_override_mcf,
        test_gas_volume_mcf,

        -- gas lift
        recovered_lift_gas_mcf,
        total_gas_rate_mcf_per_day,
        lift_gas_rate_mcf_per_day,

        -- oil emulsion measurements
        start_reading_oil_emulsion,
        end_reading_oil_emulsion,
        tank_start_level,
        tank_end_level,
        tank_free_water_start_level,
        tank_free_water_end_level,
        tank_start_volume_bbl,
        tank_end_volume_bbl,
        oil_emul_bsw_pct,
        oil_emul_sand_cut_pct,
        gas_in_solution_factor_mcf_per_bbl,
        oil_emulsion_volume_entry_bbl,
        oil_emulsion_volume_override_bbl,
        gas_equivalent_of_liquids_volume_mcf,
        test_total_fluid_volume_bbl,
        test_oil_condensate_volume_bbl,
        oil_cond_vol_before_tp_correction_bbl,

        -- water measurements
        water_start_reading,
        water_end_reading,
        water_volume_entry_bbl,
        water_volume_override_bbl,
        test_water_volume_bbl,

        -- sand measurements
        sand_volume_entry_bbl,
        sand_volume_override_bbl,
        test_sand_volume_bbl,

        -- test results - rates
        produced_gas_rate_mcf_per_day,
        total_fluid_rate_bbl_per_day,
        oil_cond_rate_bbl_per_day,
        gas_equiv_of_oil_cond_rate_mcf_per_day,
        water_rate_bbl_per_day,
        sand_rate_bbl_per_day,

        -- test results - quality metrics
        total_bsw_pct,
        total_sand_cut_pct,
        gas_oil_ratio_mcf_per_bbl,
        condensate_gas_ratio_bbl_per_mcf,
        water_gas_ratio_bbl_per_mcf,

        -- test conditions - pressures
        pressure_bh_psi,
        casing_pressure_psi,
        pressure_inject_gas_lift_gas_psi,
        pressure_prod_sep_psi,
        pressure_test_separator_psi,
        pressure_wellhead_psi,

        -- test conditions - temperatures
        temperature_bottom_hole_f,
        temperature_production_separator_f,
        temperature_test_separator_f,
        temperature_well_head_f,

        -- previous test comparisons - rate changes
        change_in_oil_emulsion_rate_bbl_per_day,
        pct_change_in_oil_emulsion_rate_pct,
        change_in_gas_rate_mcf_per_day,
        pct_change_in_gas_rate_pct,
        change_in_water_rate_bbl_per_day,
        pct_change_in_water_rate_pct,
        change_in_sand_rate_bbl_per_day,
        pct_change_in_sand_rate_pct,

        -- previous test comparisons - ratio changes
        change_in_gor_mcf_per_bbl,
        pct_change_in_gor_pct,
        change_in_cgr_bbl_per_mcf,
        pct_change_in_cgr_pct,
        change_in_wgr_bbl_per_mcf,
        pct_change_in_wgr_pct,
        change_in_bsw_pct,
        pct_change_in_bsw_pct,
        change_in_sand_cut_pct,
        pct_change_in_sand_cut_pct,
        reason_for_variance,

        -- reference information
        data_source,
        allocation_flag,
        deliverability_flag,
        regulatory_flag,

        -- temperature and pressure correction
        temperature_of_opening_level_f,
        pressure_of_opening_level_psi,
        temperature_of_closing_level_f,
        pressure_of_closing_level_psi,
        temperature_of_sample_f,
        pressure_of_sample_psi,
        density_of_sample_api,
        density_of_sample_at_60f_api,

        -- regulatory properties
        shut_in_wellhead_pressure_psi,
        flowing_wellhead_pressure_psi,
        bradenhead_pressure_psi,
        bradenhead_reason,
        gas_specific_gravity,
        condensate_gravity_api,

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
        user_num5,
        user_date_1,
        user_date_2,
        user_date_3,
        user_date_4,
        user_date_5,

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
