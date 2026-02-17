{{
    config(
        materialized='view',
        tags=['hubspot', 'staging', 'formentera']
    )
}}

with

-- 1. SOURCE: Raw data from source. No dedup needed for Portable.
source as (
    select * from {{ source('hubspot', 'companies') }}
),

-- 2. RENAMED: Column renaming, type casting, trimming. No filtering, no logic.
renamed as (
    select
        -- identifiers
        trim(id)::varchar as company_id,

        -- descriptive fields
        {{ clean_null_string('properties:NAME') }}::varchar as company_name,
        {{ clean_null_string('properties:DESCRIPTION') }}::varchar as description,

        -- address
        {{ clean_null_string('properties:ADDRESS_LINE_ONE') }}::varchar as address_line_one,
        {{ clean_null_string('properties:ADDRESS_LINE_TWO') }}::varchar as address_line_two,
        {{ clean_null_string('properties:CITY') }}::varchar as city,
        {{ clean_null_string('properties:STATE') }}::varchar as state,
        {{ clean_null_string('properties:ZIP') }}::varchar as zip,
        {{ clean_null_string('properties:COUNTRY') }}::varchar as country,

        -- contact info
        {{ clean_null_string('properties:EMAIL') }}::varchar as email,
        {{ clean_null_string('properties:PHONE') }}::varchar as phone,

        -- financial / tax
        {{ clean_null_string('properties:TAX_STATUS') }}::varchar as tax_status,
        properties:TAX_EXEMPT::boolean as is_tax_exempt,
        {{ clean_null_string('properties:TAX_ID_NUMBER') }}::varchar as tax_id_number,
        {{ clean_null_string('properties:REVENUE_PAYMENT_TYPE') }}::varchar as revenue_payment_type,
        try_to_number({{ clean_null_string('properties:MINIMUM_CHECK_AMOUNT') }}, 10, 2) as minimum_check_amount,
        {{ clean_null_string('properties:DBA_1099_NAME') }}::varchar as dba_1099_name,
        {{ clean_null_string('properties:FEDERAL_WITHHOLDING') }}::varchar as federal_withholding,
        {{ clean_null_string('properties:STATE_WITHHOLDING') }}::varchar as state_withholding,

        -- operations
        {{ clean_null_string('properties:OWNER_NUMBER___OGSYS') }}::varchar as owner_number_ogsys,
        {{ clean_null_string('properties:ACQUIRED_OWNER_NUMBER') }}::varchar as acquired_owner_number,
        {{ clean_null_string('properties:ASSET') }}::varchar as asset,
        {{ clean_null_string('properties:ENERGY_LINK_UPLOAD') }}::varchar as energy_link_upload,
        {{ clean_null_string('properties:STATEMENT_DELIVERY') }}::varchar as statement_delivery,
        {{ clean_null_string('properties:REVENUE_CHECK_STUB') }}::varchar as revenue_check_stub,
        {{ clean_null_string('properties:NETTING_CODE') }}::varchar as netting_code,
        {{ clean_null_string('properties:NETTING_CODE_HELPER') }}::varchar as netting_code_helper,
        {{ clean_null_string('properties:MINIMUM_JIB') }}::varchar as minimum_jib,
        {{ clean_null_string('properties:SUSPENSE_REASON') }}::varchar as suspense_reason,

        -- flags
        coalesce(properties:ACTIVE::boolean, false) as is_active,
        coalesce(properties:WORKING_INTEREST_OWNER::boolean, false) as is_working_interest_owner,
        coalesce(properties:HOLD_REVENUE_CHECKS::boolean, false) as is_revenue_checks_on_hold,
        archived::boolean as is_archived,

        -- hubspot
        {{ clean_null_string('properties:HUBSPOT_OWNER_ID') }}::varchar as hubspot_owner_id,
        {{ clean_null_string('properties:LIFECYCLESTAGE') }}::varchar as lifecycle_stage,
        try_to_number({{ clean_null_string('properties:NUM_ASSOCIATED_CONTACTS') }}) as num_associated_contacts,
        coalesce(properties:HS_WAS_IMPORTED::boolean, false) as is_imported,

        -- dates
        try_to_timestamp({{ clean_null_string('properties:CREATEDATE') }}) as created_at,
        try_to_timestamp({{ clean_null_string('properties:HS_LASTMODIFIEDDATE') }}) as last_modified_at,

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
        and company_id is not null
),

-- 4. ENHANCED: Add surrogate key + _loaded_at.
enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['company_id']) }} as company_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This IS the contract.
final as (
    select
        -- surrogate key
        company_sk,

        -- identifiers
        company_id,

        -- descriptive fields
        company_name,
        description,

        -- address
        address_line_one,
        address_line_two,
        city,
        state,
        zip,
        country,

        -- contact info
        email,
        phone,

        -- financial / tax
        tax_status,
        is_tax_exempt,
        tax_id_number,
        revenue_payment_type,
        minimum_check_amount,
        dba_1099_name,
        federal_withholding,
        state_withholding,

        -- operations
        owner_number_ogsys,
        acquired_owner_number,
        asset,
        energy_link_upload,
        statement_delivery,
        revenue_check_stub,
        netting_code,
        netting_code_helper,
        minimum_jib,
        suspense_reason,

        -- flags
        is_active,
        is_working_interest_owner,
        is_revenue_checks_on_hold,
        is_archived,
        is_imported,

        -- hubspot
        hubspot_owner_id,
        lifecycle_stage,
        num_associated_contacts,

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
