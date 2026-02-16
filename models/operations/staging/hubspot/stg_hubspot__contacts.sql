{{
    config(
        materialized='view',
        tags=['hubspot', 'staging', 'formentera']
    )
}}

with

-- 1. SOURCE: Raw data from source. No dedup needed for Portable.
source as (
    select * from {{ source('hubspot', 'contacts') }}
),

-- 2. RENAMED: Column renaming, type casting, trimming. No filtering, no logic.
renamed as (
    select
        -- identifiers
        trim(id)::varchar as contact_id,
        {{ clean_null_string('properties:ASSOCIATEDCOMPANYID') }}::varchar as associated_company_id,

        -- descriptive fields
        {{ clean_null_string('properties:FIRSTNAME') }}::varchar as first_name,
        {{ clean_null_string('properties:LASTNAME') }}::varchar as last_name,
        {{ clean_null_string('properties:EMAIL') }}::varchar as email,
        {{ clean_null_string('properties:HS_EMAIL_DOMAIN') }}::varchar as email_domain,
        {{ clean_null_string('properties:PHONE') }}::varchar as phone,
        {{ clean_null_string('properties:MOBILEPHONE') }}::varchar as mobile_phone,
        {{ clean_null_string('properties:JOBTITLE') }}::varchar as job_title,

        -- address
        {{ clean_null_string('properties:ADDRESS') }}::varchar as address,
        {{ clean_null_string('properties:CITY') }}::varchar as city,
        {{ clean_null_string('properties:STATE') }}::varchar as state,
        {{ clean_null_string('properties:ZIP') }}::varchar as zip,
        {{ clean_null_string('properties:COUNTRY') }}::varchar as country,

        -- revenue / statements
        {{ clean_null_string('properties:REV_STATEMENT_DELIVERY') }}::varchar as rev_statement_delivery,
        {{ clean_null_string('properties:REV_STATEMENT_EMAIL') }}::varchar as rev_statement_email,

        -- flags
        archived::boolean as is_archived,
        coalesce(properties:HS_WAS_IMPORTED::boolean, false) as is_imported,

        -- hubspot
        {{ clean_null_string('properties:HUBSPOT_OWNER_ID') }}::varchar as hubspot_owner_id,
        {{ clean_null_string('properties:LIFECYCLESTAGE') }}::varchar as lifecycle_stage,

        -- dates
        try_to_timestamp({{ clean_null_string('properties:CREATEDATE') }}) as created_at,
        try_to_timestamp({{ clean_null_string('properties:LASTMODIFIEDDATE') }}) as last_modified_at,

        -- ingestion metadata
        _portable_extracted::timestamp_tz as _portable_extracted

    from source
),

-- 3. FILTERED: Remove archived records and null PKs. No transformations.
filtered as (
    select *
    from renamed
    where
        not is_archived
        and contact_id is not null
),

-- 4. ENHANCED: Add surrogate key + _loaded_at.
enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['contact_id']) }} as contact_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This IS the contract.
final as (
    select
        -- surrogate key
        contact_sk,

        -- identifiers
        contact_id,
        associated_company_id,

        -- descriptive fields
        first_name,
        last_name,
        email,
        email_domain,
        phone,
        mobile_phone,
        job_title,

        -- address
        address,
        city,
        state,
        zip,
        country,

        -- revenue / statements
        rev_statement_delivery,
        rev_statement_email,

        -- flags
        is_archived,
        is_imported,

        -- hubspot
        hubspot_owner_id,
        lifecycle_stage,

        -- dates
        created_at,
        last_modified_at,

        -- ingestion metadata
        _portable_extracted,

        -- dbt metadata
        _loaded_at

    from enhanced
)

select * from final
