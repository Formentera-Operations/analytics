{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'reservoir_tests']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran dedup on idrec (one row per test result)
source as (
    select * from {{ source('wellview_calcs', 'WVT_WVWELLTESTTRANSRESULT') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

-- 2. RENAMED: Column renaming, type casting, trimming. No filtering, no logic.
renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as well_test_result_id,
        trim(idrecparent)::varchar as well_test_id,
        trim(idwell)::varchar as well_id,
        sysseq::float as sequence_number,

        -- descriptive fields
        trim(analysismethod)::varchar as analysis_method,
        trim(analysissoftware)::varchar as analysis_software,
        trim(analyst)::varchar as analyst_name,
        trim(analysiscom)::varchar as analysis_comments,
        trim(resboundtyp)::varchar as reservoir_boundary_type,
        trim(resboundnote)::varchar as reservoir_boundary_notes,

        -- measurements - pressure (converted from kPa to PSI)
        {{ wv_kpa_to_psi('presresmpp') }} as mpp_pressure_psi,
        {{ wv_kpa_to_psi('presresdatum') }} as datum_pressure_psi,

        -- measurements - depth (converted from meters to feet)
        {{ wv_meters_to_feet('depthmpp') }} as mpp_depth_ft,
        {{ wv_meters_to_feet('depthtvdmppcalc') }} as mpp_depth_tvd_ft,
        {{ wv_meters_to_feet('investradius') }} as investigation_radius_ft,

        -- measurements - temperature (converted from Celsius to Fahrenheit)
        {{ wv_celsius_to_fahrenheit('tempres') }} as reservoir_temperature_f,

        -- measurements - permeability (converted from sq meters to darcy)
        {{ wv_sqm_to_darcy('respermhor') }} as horizontal_permeability_darcy,

        -- measurements - productivity (converted to field units)
        respermratio::float as permeability_ratio,
        productivitycoef::float as productivity_coefficient,
        productivityexp::float as productivity_exponent,
        productivityindex / 0.0230591575847658 as productivity_index_bbl_day_psi,
        spicalc / 0.0756534073644655 as specific_productivity_index_bbl_day_ft_psi,

        -- measurements - reservoir characterization
        skin::float as skin_factor,
        mobilityratio::float as mobility_ratio,

        -- measurements - AOF (converted from cbm/day to MCF/day)
        {{ wv_cbm_to_mcf('aof') }} as aof_mcf_per_day,

        -- flags
        definitive::boolean as is_definitive_test,

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
        and well_test_result_id is not null
),

-- 4. ENHANCED: Add surrogate key + _loaded_at.
enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['well_test_result_id']) }} as well_test_result_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        well_test_result_sk,

        -- identifiers
        well_test_result_id,
        well_test_id,
        well_id,
        sequence_number,

        -- descriptive fields
        analysis_method,
        analysis_software,
        analyst_name,
        analysis_comments,
        reservoir_boundary_type,
        reservoir_boundary_notes,

        -- measurements - pressure
        mpp_pressure_psi,
        datum_pressure_psi,

        -- measurements - depth
        mpp_depth_ft,
        mpp_depth_tvd_ft,
        investigation_radius_ft,

        -- measurements - temperature
        reservoir_temperature_f,

        -- measurements - permeability
        horizontal_permeability_darcy,

        -- measurements - productivity
        permeability_ratio,
        productivity_coefficient,
        productivity_exponent,
        productivity_index_bbl_day_psi,
        specific_productivity_index_bbl_day_ft_psi,

        -- measurements - reservoir characterization
        skin_factor,
        mobility_ratio,

        -- measurements - AOF
        aof_mcf_per_day,

        -- flags
        is_definitive_test,

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
