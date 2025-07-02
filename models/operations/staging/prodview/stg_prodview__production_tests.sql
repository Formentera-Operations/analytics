{{ config(
    materialized='view',
    tags=['prodview', 'tests', 'completions', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPTEST') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as test_id,
        idrecparent as completion_id,
        idflownet as flow_network_id,
        
        -- Test information
        typ as test_type,
        dttm as effective_date,
        durtest / 0.0416666666666667 as test_hours,
        dontuse as invalid_test,
        testedby as tested_by,
        com as note,
        trailerduration as trailer_duration_days,
        
        -- Separator reference
        idrecsep as separator_vessel_id,
        idrecseptk as separator_vessel_table,
        idrecsepunitcalc as separator_vessel_unit_calc_id,
        idrecsepunitcalctk as separator_vessel_unit_calc_table,
        
        -- Equipment conditions
        szchoke / 0.000396875 as choke_size_64ths,
        
        -- Gas measurements
        cprime as c_prime,
        duronor / 0.0416666666666667 as hours_on_override,
        presgasstatic as orifice_static_pressure_gauge,
        presgasdiff as orifice_differential_pressure,
        tempgas as orifice_temperature,
        szorifice as orifice_size,
        volentergas / 28.316846592 as gas_volume_entry_mcf,
        volgas / 28.316846592 as gas_volume_override_mcf,
        volgascalc / 28.316846592 as test_gas_volume_mcf,
        
        -- Gas lift
        volliftgasrecov / 28.316846592 as recovered_lift_gas_mcf,
        ratetotalgascalc / 28.316846592 as total_gas_rate_mcf_per_day,
        rateliftgascalc / 28.316846592 as lift_gas_rate_mcf_per_day,
        
        -- Oil emulsion measurements
        readinghcliqstart as start_reading_oil_emulsion,
        readinghcliqend as end_reading_oil_emulsion,
        leveltankstart as tank_start_level,
        leveltankend as tank_end_level,
        leveltankfreewaterstart as tank_free_water_start_level,
        leveltankfreewaterend as tank_free_water_end_level,
        voltankstartcalc / 0.158987294928 as tank_start_volume_bbl,
        voltankendcalc / 0.158987294928 as tank_end_volume_bbl,
        bsw / 0.01 as oil_emul_bsw_pct,
        sandcut / 0.01 as oil_emul_sand_cut_pct,
        factgasinsoln / 178.107606679035 as gas_in_solution_factor_mcf_per_bbl,
        volenterhcliq / 0.158987294928 as oil_emulsion_volume_entry_bbl,
        volhcliq / 0.158987294928 as oil_emulsion_volume_override_bbl,
        volhcliqgaseqcalc / 28.316846592 as gas_equivalent_of_liquids_volume_mcf,
        volfluidtotalcalc / 0.158987294928 as test_total_fluid_volume_bbl,
        volhcliqcalc / 0.158987294928 as test_oil_condensate_volume_bbl,
        volbeforetpcorrhcliqcalc / 0.158987294928 as oil_cond_vol_before_tp_correction_bbl,
        
        -- Water measurements
        readingwaterstart as water_start_reading,
        readingwaterend as water_end_reading,
        volenterwater / 0.158987294928 as water_volume_entry_bbl,
        volwater / 0.158987294928 as water_volume_override_bbl,
        volwatercalc / 0.158987294928 as test_water_volume_bbl,
        
        -- Sand measurements
        volentersand / 0.158987294928 as sand_volume_entry_bbl,
        volsand / 0.158987294928 as sand_volume_override_bbl,
        volsandcalc / 0.158987294928 as test_sand_volume_bbl,
        
        -- Test results - Rates (converted to per day)
        rateprodgascalc / 28.316846592 as produced_gas_rate_mcf_per_day,
        ratefluidtotalcalc / 0.1589873 as total_fluid_rate_bbl_per_day,
        ratehcliqcalc / 0.1589873 as oil_cond_rate_bbl_per_day,
        ratehcliqgaseqcalc / 28.316846592 as gas_equiv_of_oil_cond_rate_mcf_per_day,
        ratewatercalc / 0.1589873 as water_rate_bbl_per_day,
        ratesandcalc / 0.1589873 as sand_rate_bbl_per_day,
        
        -- Test results - Quality metrics
        bswtotalcalc / 0.01 as total_bsw_pct,
        sandcuttotalcalc / 0.01 as total_sand_cut_pct,
        gorcalc / 178.107606679035 as gas_oil_ratio_mcf_per_bbl,
        cgrcalc / 0.00561458333333333 as condensate_gas_ratio_bbl_per_mcf,
        wgrcalc / 0.00561458333333333 as water_gas_ratio_bbl_per_mcf,
        
        -- Test conditions - Pressures (converted to PSI)
        presbh / 6.894757 as pressure_bh_psi,
        prescas / 6.894757 as casing_pressure_psi,
        presinjectgasliftgas / 6.894757 as pressure_inject_gas_lift_gas_psi,
        presprodsep / 6.894757 as pressure_prod_sep_psi,
        prestestsep / 6.894757 as pressure_test_separator_psi,
        preswh / 6.894757 as pressure_wellhead_psi,
        
        -- Test conditions - Temperatures (converted to Fahrenheit)
        tempbh / 0.555555555555556 + 32 as temperature_bottom_hole_f,
        tempprodsep / 0.555555555555556 + 32 as temperature_production_separator_f,
        temptestsep / 0.555555555555556 + 32 as temperature_test_separator_f,
        tempwh / 0.555555555555556 + 32 as temperature_well_head_f,
        
        -- Previous test comparisons - Rate changes
        ratechghcliqcalc / 0.1589873 as change_in_oil_emulsion_rate_bbl_per_day,
        pctchghcliqcalc / 0.01 as pct_change_in_oil_emulsion_rate_pct,
        ratechggascalc / 28.316846592 as change_in_gas_rate_mcf_per_day,
        pctchggascalc / 0.01 as pct_change_in_gas_rate_pct,
        ratechgwatercalc / 0.1589873 as change_in_water_rate_bbl_per_day,
        pctchgwatercalc / 0.01 as pct_change_in_water_rate_pct,
        ratechgsandcalc / 0.1589873 as change_in_sand_rate_bbl_per_day,
        pctchgsandcalc / 0.01 as pct_change_in_sand_rate_pct,
        
        -- Previous test comparisons - Ratio changes
        chggorcalc / 178.107606679035 as change_in_gor_mcf_per_bbl,
        pctchggorcalc / 0.01 as pct_change_in_gor_pct,
        chgcgrcalc / 0.00561458333333333 as change_in_cgr_bbl_per_mcf,
        pctchgcgrcalc / 0.01 as pct_change_in_cgr_pct,
        chgwgrcalc / 0.00561458333333333 as change_in_wgr_bbl_per_mcf,
        pctchgwgrcalc / 0.01 as pct_change_in_wgr_pct,
        chgbswcalc / 0.01 as change_in_bsw_pct,
        pctchgbswcalc / 0.01 as pct_change_in_bsw_pct,
        chgsandcutcalc / 0.01 as change_in_sand_cut_pct,
        pctchgsandcutcalc / 0.01 as pct_change_in_sand_cut_pct,
        reasonvariance as reason_for_variance,
        
        -- Reference information
        datasource as data_source,
        purposealloc as allocation_flag,
        purposedeliv as deliverability_flag,
        purposereg as regulatory_flag,
        
        -- Temperature and pressure correction
        tempstart / 0.555555555555556 + 32 as temperature_of_opening_level_f,
        presstart / 6.894757 as pressure_of_opening_level_psi,
        tempend / 0.555555555555556 + 32 as temperature_of_closing_level_f,
        presend / 6.894757 as pressure_of_closing_level_psi,
        tempsample / 0.555555555555556 + 32 as temperature_of_sample_f,
        pressample / 6.894757 as pressure_of_sample_psi,
        power(nullif(densitysample, 0), -1) / 7.07409872233005E-06 + -131.5 as density_of_sample_api,
        power(nullif(densitysample60f, 0), -1) / 7.07409872233005E-06 + -131.5 as density_of_sample_at_60f_api,
        
        -- Regulatory properties
        preswhsi / 6.894757 as shut_in_wellhead_pressure_psi,
        preswhflow / 6.894757 as flowing_wellhead_pressure_psi,
        presbradenhead / 6.894757 as bradenhead_pressure_psi,
        reasonbradenhead as bradenhead_reason,
        densityrelgas as gas_specific_gravity,
        power(nullif(densitycond, 0), -1) / 7.07409872233005E-06 + -131.5 as condensate_gravity_api,
        
        -- User-defined fields - Text
        usertxt1 as user_text_1,
        usertxt2 as user_text_2,
        usertxt3 as user_text_3,
        usertxt4 as user_text_4,
        usertxt5 as user_text_5,
        
        -- User-defined fields - Numeric
        usernum1 as user_num_1,
        usernum2 as user_num_2,
        usernum3 as user_num_3,
        usernum4 as user_num_4,
        usernum5 as user_num_5,
        
        -- User-defined fields - Datetime
        userdttm1 as user_date_1,
        userdttm2 as user_date_2,
        userdttm3 as user_date_3,
        userdttm4 as user_date_4,
        userdttm5 as user_date_5,
        
        -- System fields
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,
        syslockdate as system_lock_date,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        
        -- Fivetran fields
        _fivetran_synced as fivetran_synced_at
        
    from source_data
)

select * from renamed