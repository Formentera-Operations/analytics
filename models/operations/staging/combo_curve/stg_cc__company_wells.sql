{{
    config(
        materialized='view',
        tags=['combo_curve', 'staging', 'formentera']
    )
}}

with

-- 1. SOURCE: Raw data from source. No dedup needed for Portable.
source as (
    select * from {{ source('combo_curve', 'wells') }}
),

-- 2. RENAMED: Column renaming, type casting, trimming. No filtering, no logic.
renamed as (
    select
        -- identifiers
        trim(id)::varchar as well_id,
        trim(ariesid)::varchar as aries_id,
        trim(phdwinid)::varchar as phdwin_id,
        trim(chosenid)::varchar as chosen_id,

        -- descriptive fields
        trim(wellname)::varchar as well_name,
        trim(wellnumber)::varchar as well_number,
        trim(welltype)::varchar as well_type,
        trim(leasename)::varchar as lease_name,
        trim(api10)::varchar as api_10,
        trim(api12)::varchar as api_12,
        trim(api14)::varchar as api_14,
        trim(currentoperator)::varchar as operator,
        trim(currentoperatorcode)::varchar as operator_code,
        trim(customstring3)::varchar as operator_category,
        trim(status)::varchar as status,
        trim(primaryproduct)::varchar as primary_product,
        trim(customstring1)::varchar as reserve_category,
        trim(customstring22)::varchar as company_name,

        -- location
        trim(basin)::varchar as basin,
        regexp_replace(county, '\\s*\\([A-Z]{2}\\)$', '')::varchar as county,
        trim(state)::varchar as state,

        -- measures
        surfacelatitude::float as surface_latitude,
        surfacelongitude::float as surface_longitude,
        measureddepth::float as measured_depth,
        trueverticaldepth::float as true_vertical_depth,
        laterallength::float as lateral_length,

        -- flags
        hasdaily::boolean as has_daily,
        hasmonthly::boolean as has_monthly,

        -- system / audit
        trim(datapool)::varchar as data_pool,
        trim(datasource)::varchar as data_source,

        -- dates
        createdat::timestamp_ntz as created_at,
        updatedat::timestamp_ntz as updated_at,

        -- ingestion metadata
        _portable_extracted::timestamp_tz as _portable_extracted

    from source
),

-- 3. FILTERED: Remove null PKs. No transformations.
filtered as (
    select *
    from renamed
    where well_id is not null
),

-- 4. ENHANCED: Add surrogate key + _loaded_at. Computed flags.
enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['well_id']) }} as company_well_sk,
        *,
        operator_category = 'OP' as is_operated,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This IS the contract.
final as (
    select
        -- surrogate key
        company_well_sk,

        -- identifiers
        well_id,
        aries_id,
        phdwin_id,
        chosen_id,

        -- descriptive fields
        well_name,
        well_number,
        well_type,
        lease_name,
        api_10,
        api_12,
        api_14,
        operator,
        operator_code,
        operator_category,
        status,
        primary_product,
        reserve_category,
        company_name,

        -- location
        basin,
        county,
        state,

        -- measures
        surface_latitude,
        surface_longitude,
        measured_depth,
        true_vertical_depth,
        lateral_length,

        -- flags
        is_operated,
        has_daily,
        has_monthly,

        -- system / audit
        data_pool,
        data_source,

        -- dates
        created_at,
        updated_at,

        -- ingestion metadata
        _portable_extracted,

        -- dbt metadata
        _loaded_at

    from enhanced
)

select * from final
