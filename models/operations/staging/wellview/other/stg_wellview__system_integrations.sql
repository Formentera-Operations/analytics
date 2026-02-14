{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'other']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran dedup on idrec (one row per integration record)
source as (
    select * from {{ source('wellview', 'WVT_WVSYSINTEGRATION') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

-- 2. RENAMED: Column renaming, type casting, trimming. No filtering, no logic.
renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as system_integration_id,
        trim(idrecparent)::varchar as parent_record_id,
        trim(idwell)::varchar as well_id,
        trim(tblkeyparent)::varchar as parent_table_key,

        -- descriptive fields
        trim(integratordes)::varchar as integrator_description,
        trim(integratorver)::varchar as integrator_version,
        trim(afproduct)::varchar as product_description,
        trim(afidentity)::varchar as af_entity_id,
        trim(afidrec)::varchar as af_record_id,
        trim(note)::varchar as integration_notes,

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
        and system_integration_id is not null
),

-- 4. ENHANCED: Add surrogate key + _loaded_at.
enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['system_integration_id']) }} as system_integration_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        system_integration_sk,

        -- identifiers
        system_integration_id,
        parent_record_id,
        well_id,
        parent_table_key,

        -- descriptive fields
        integrator_description,
        integrator_version,
        product_description,
        af_entity_id,
        af_record_id,
        integration_notes,

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
