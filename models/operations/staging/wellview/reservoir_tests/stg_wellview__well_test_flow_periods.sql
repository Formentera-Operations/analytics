{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'reservoir_tests']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran dedup on idrec (one row per flow period)
source as (
    select * from {{ source('wellview_calcs', 'WVT_WVWELLTESTTRANSFLOWPER') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

-- 2. RENAMED: Column renaming, type casting, trimming. No filtering, no logic.
renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as flow_period_id,
        trim(idrecparent)::varchar as well_test_id,
        trim(idwell)::varchar as well_id,
        trim(idrecgaugeused)::varchar as gauge_used_id,
        trim(idrecgaugeusedtk)::varchar as gauge_used_table_key,

        -- descriptive fields
        trim(typ)::varchar as flow_period_type,
        trim(com)::varchar as comments,

        -- measurements - timing
        {{ wv_days_to_hours('durcalc') }} as duration_hours,

        -- measurements - choke and operational
        szdiachoke / 0.000396875 as choke_diameter_64ths,
        efficiency / 0.01 as efficiency_percent,

        -- measurements - fluid level depths (converted from meters to feet)
        {{ wv_meters_to_feet('depthfluidlevelend') }} as fluid_level_end_depth_ft,
        {{ wv_meters_to_feet('depthtvdfluidlevelendcalc') }} as fluid_level_end_depth_tvd_ft,

        -- measurements - initial pressures (converted from kPa to PSI)
        {{ wv_kpa_to_psi('presbhinit') }} as bottomhole_pressure_initial_psi,
        {{ wv_kpa_to_psi('prescasinit') }} as casing_pressure_initial_psi,
        {{ wv_kpa_to_psi('prestubinit') }} as tubing_pressure_initial_psi,
        {{ wv_kpa_to_psi('pressepinit') }} as separator_pressure_initial_psi,

        -- measurements - end pressures (converted from kPa to PSI)
        {{ wv_kpa_to_psi('presbhend') }} as bottomhole_pressure_end_psi,
        {{ wv_kpa_to_psi('prescasend') }} as casing_pressure_end_psi,
        {{ wv_kpa_to_psi('prestubend') }} as tubing_pressure_end_psi,
        {{ wv_kpa_to_psi('pressepend') }} as separator_pressure_end_psi,

        -- measurements - initial temperatures (converted from Celsius to Fahrenheit)
        {{ wv_celsius_to_fahrenheit('tempbhinit') }} as bottomhole_temperature_initial_f,
        {{ wv_celsius_to_fahrenheit('tempcasinit') }} as casing_temperature_initial_f,
        {{ wv_celsius_to_fahrenheit('temptubinit') }} as tubing_temperature_initial_f,
        {{ wv_celsius_to_fahrenheit('tempsepinit') }} as separator_temperature_initial_f,

        -- measurements - end temperatures (converted from Celsius to Fahrenheit)
        {{ wv_celsius_to_fahrenheit('tempbhend') }} as bottomhole_temperature_end_f,
        {{ wv_celsius_to_fahrenheit('tempcasend') }} as casing_temperature_end_f,
        {{ wv_celsius_to_fahrenheit('temptubend') }} as tubing_temperature_end_f,
        {{ wv_celsius_to_fahrenheit('tempsepend') }} as separator_temperature_end_f,

        -- measurements - production rates (converted to field units)
        {{ wv_cbm_per_day_to_bbl_per_day('rateoilend') }} as oil_rate_end_bbl_per_day,
        {{ wv_cbm_per_day_to_bbl_per_day('ratewaterend') }} as water_rate_end_bbl_per_day,
        {{ wv_cbm_per_day_to_bbl_per_day('ratecondend') }} as condensate_rate_end_bbl_per_day,
        {{ wv_cbm_to_mcf('rategasend') }} as gas_rate_end_mcf_per_day,
        {{ wv_cbm_to_mcf('rategasliftgas') }} as gas_lift_rate_mcf_per_day,

        -- measurements - total volumes (converted to field units)
        {{ wv_cbm_to_bbl('volumeoiltotal') }} as total_oil_volume_bbl,
        {{ wv_cbm_to_bbl('volumewatertotal') }} as total_water_volume_bbl,
        {{ wv_cbm_to_bbl('volumecondtotal') }} as total_condensate_volume_bbl,
        {{ wv_cbm_to_mcf('volumegastotal') }} as total_gas_volume_mcf,

        -- measurements - fluid properties
        bswend / 0.01 as basic_sediments_water_end_percent,
        sandcutend / 0.01 as sand_cut_end_percent,
        salinitywaterend / 1e-06 as water_salinity_end_ppm,
        h2send / 1e-06 as h2s_end_ppm,
        phwaterend::float as water_ph_end,

        -- measurements - density
        power(nullif(densityoilend, 0), -1) / 7.07409872233005e-06 - 131.5 as oil_density_end_api,
        densitygasend / 0.01601846250554 as gas_density_end_lb_per_1000ft3,

        -- dates
        dttmstart::timestamp_ntz as flow_period_start_date,
        dttmend::timestamp_ntz as flow_period_end_date,

        -- system / audit
        syscreatedate::timestamp_ntz as created_at_utc,
        trim(syscreateuser)::varchar as created_by,
        sysmoddate::timestamp_ntz as last_mod_at_utc,
        trim(sysmoduser)::varchar as last_mod_by,
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

-- 3. FILTERED: Remove soft deletes and null PKs. No transformations.
filtered as (
    select *
    from renamed
    where
        coalesce(_fivetran_deleted, false) = false
        and flow_period_id is not null
),

-- 4. ENHANCED: Add surrogate key + _loaded_at.
enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['flow_period_id']) }} as flow_period_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        flow_period_sk,

        -- identifiers
        flow_period_id,
        well_test_id,
        well_id,
        gauge_used_id,
        gauge_used_table_key,

        -- descriptive fields
        flow_period_type,
        comments,

        -- measurements - timing
        duration_hours,

        -- measurements - choke and operational
        choke_diameter_64ths,
        efficiency_percent,

        -- measurements - fluid level depths
        fluid_level_end_depth_ft,
        fluid_level_end_depth_tvd_ft,

        -- measurements - initial pressures
        bottomhole_pressure_initial_psi,
        casing_pressure_initial_psi,
        tubing_pressure_initial_psi,
        separator_pressure_initial_psi,

        -- measurements - end pressures
        bottomhole_pressure_end_psi,
        casing_pressure_end_psi,
        tubing_pressure_end_psi,
        separator_pressure_end_psi,

        -- measurements - initial temperatures
        bottomhole_temperature_initial_f,
        casing_temperature_initial_f,
        tubing_temperature_initial_f,
        separator_temperature_initial_f,

        -- measurements - end temperatures
        bottomhole_temperature_end_f,
        casing_temperature_end_f,
        tubing_temperature_end_f,
        separator_temperature_end_f,

        -- measurements - production rates
        oil_rate_end_bbl_per_day,
        water_rate_end_bbl_per_day,
        condensate_rate_end_bbl_per_day,
        gas_rate_end_mcf_per_day,
        gas_lift_rate_mcf_per_day,

        -- measurements - total volumes
        total_oil_volume_bbl,
        total_water_volume_bbl,
        total_condensate_volume_bbl,
        total_gas_volume_mcf,

        -- measurements - fluid properties
        basic_sediments_water_end_percent,
        sand_cut_end_percent,
        water_salinity_end_ppm,
        h2s_end_ppm,
        water_ph_end,

        -- measurements - density
        oil_density_end_api,
        gas_density_end_lb_per_1000ft3,

        -- dates
        flow_period_start_date,
        flow_period_end_date,

        -- system / audit
        created_at_utc,
        created_by,
        last_mod_at_utc,
        last_mod_by,
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
