{{
    config(
        materialized='view',
        tags=['combo_curve', 'staging', 'formentera']
    )
}}

with

-- 1. SOURCE: Raw data from source. No dedup needed for Portable.
source as (
    select * from {{ source('combo_curve', 'projects') }}
),

-- 2. RENAMED: Column renaming, type casting, trimming. No filtering, no logic.
renamed as (
    select
        -- identifiers
        trim(id)::varchar as project_id,

        -- descriptive fields
        trim(name)::varchar as project_name,

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
    where project_id is not null
),

-- 4. ENHANCED: Add surrogate key + _loaded_at.
enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['project_id']) }} as project_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This IS the contract.
final as (
    select
        -- surrogate key
        project_sk,

        -- identifiers
        project_id,

        -- descriptive fields
        project_name,

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
