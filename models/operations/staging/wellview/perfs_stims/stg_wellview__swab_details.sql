{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'perfs_stims']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran dedup on idrec (one row per swab detail record)
source as (
    select * from {{ source('wellview_calcs', 'WVT_WVSWABDETAILS') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

-- 2. RENAMED: Column renaming, type casting, trimming. No filtering, no logic.
renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as swab_detail_id,
        trim(idrecparent)::varchar as swab_id,
        trim(idwell)::varchar as well_id,

        -- descriptive fields
        swabno::float as swab_number,
        ph::float as ph_value,
        trim(com)::varchar as comments,

        -- duration (converted from days to hours)
        {{ wv_days_to_hours('tmswab') }} as swabbing_time_hours,

        -- pressures (converted to PSI)
        {{ wv_kpa_to_psi('prestub') }} as tubing_pressure_psi,
        {{ wv_kpa_to_psi('prescas') }} as casing_pressure_psi,

        -- temperature (converted to Fahrenheit)
        {{ wv_celsius_to_fahrenheit('tempwh') }} as wellhead_temperature_f,

        -- depths (converted to feet)
        {{ wv_meters_to_feet('depthfluidlevel') }} as tagged_fluid_level_ft,
        {{ wv_meters_to_feet('depthpull') }} as pull_depth_ft,
        {{ wv_meters_to_feet('depthtvdfluidlevelcalc') }} as fluid_level_tvd_ft,
        {{ wv_meters_to_feet('depthtvdpullcalc') }} as pull_depth_tvd_ft,

        -- tank measurements (converted to inches)
        {{ wv_meters_to_inches('tankgauge') }} as tank_gauge_inches,

        -- fluid volumes (converted to barrels)
        {{ wv_cbm_to_bbl('volfluidrec') }} as recovered_fluid_volume_bbl,
        {{ wv_cbm_to_bbl('voloilcalc') }} as oil_volume_bbl,
        {{ wv_cbm_to_bbl('volbswcalc') }} as bsw_volume_bbl,
        {{ wv_cbm_to_bbl('volcumcalc') }} as cumulative_volume_bbl,
        {{ wv_cbm_to_bbl('volcumoilcalc') }} as cumulative_oil_volume_bbl,
        {{ wv_cbm_to_bbl('volcumbswcalc') }} as cumulative_bsw_volume_bbl,

        -- gas volumes and rates (converted to MCF)
        {{ wv_cbm_to_mcf('volgas') }} as gas_volume_mcf,
        {{ wv_cbm_to_mcf('volcumgascalc') }} as cumulative_gas_volume_mcf,
        {{ wv_cbm_to_mcf('rategas') }} as gas_rate_mcf_per_day,

        -- oil rate (converted to BBL/day)
        {{ wv_cbm_per_day_to_bbl_per_day('oilratecalc') }} as oil_rate_bbl_per_day,

        -- fluid properties (inline — percent and ppm, no macro)
        bsw / 0.01 as basic_sediments_water_percent,
        sandcut / 0.01 as sand_cut_percent,
        salinity / 1e-06 as salinity_ppm,
        h2s / 1e-06 as h2s_ppm,

        -- density (inline — complex API gravity conversion)
        power(nullif(density, 0), -1) / 7.07409872233005e-06 - 131.5 as density_api,

        -- dates
        dttm::timestamp_ntz as swab_detail_date,

        -- system locking
        syslockmeui::boolean as system_lock_me_ui,
        syslockchildrenui::boolean as system_lock_children_ui,
        syslockme::boolean as system_lock_me,
        syslockchildren::boolean as system_lock_children,
        syslockdate::timestamp_ntz as system_lock_date,

        -- system / audit
        syscreatedate::timestamp_ntz as created_at_utc,
        trim(syscreateuser)::varchar as created_by,
        sysmoddate::timestamp_ntz as last_mod_at_utc,
        trim(sysmoduser)::varchar as last_mod_by,
        trim(systag)::varchar as system_tag,

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
        and swab_detail_id is not null
),

-- 4. ENHANCED: Add surrogate key + _loaded_at.
enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['swab_detail_id']) }} as swab_detail_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        swab_detail_sk,

        -- identifiers
        swab_detail_id,
        swab_id,
        well_id,

        -- descriptive fields
        swab_number,
        ph_value,
        comments,

        -- duration
        swabbing_time_hours,

        -- pressures
        tubing_pressure_psi,
        casing_pressure_psi,

        -- temperature
        wellhead_temperature_f,

        -- depths
        tagged_fluid_level_ft,
        pull_depth_ft,
        fluid_level_tvd_ft,
        pull_depth_tvd_ft,

        -- tank measurements
        tank_gauge_inches,

        -- fluid volumes
        recovered_fluid_volume_bbl,
        oil_volume_bbl,
        bsw_volume_bbl,
        cumulative_volume_bbl,
        cumulative_oil_volume_bbl,
        cumulative_bsw_volume_bbl,

        -- gas volumes and rates
        gas_volume_mcf,
        cumulative_gas_volume_mcf,
        gas_rate_mcf_per_day,

        -- oil rate
        oil_rate_bbl_per_day,

        -- fluid properties
        basic_sediments_water_percent,
        sand_cut_percent,
        salinity_ppm,
        h2s_ppm,

        -- density
        density_api,

        -- dates
        swab_detail_date,

        -- system locking
        system_lock_me_ui,
        system_lock_children_ui,
        system_lock_me,
        system_lock_children,
        system_lock_date,

        -- system / audit
        created_at_utc,
        created_by,
        last_mod_at_utc,
        last_mod_by,
        system_tag,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
