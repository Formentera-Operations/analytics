{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Joint Interest Billing (Summary)

    Source: ODA_JIB (Estuary CDC)
    Grain: One row per JIB summary line item (id)

    Notes:
    - Soft deletes filtered out (operation_type = 'd')
    - Denormalized summary view with codes/names for reporting
    - For normalized detail with FKs, see stg_oda__jibdetail
    - No direct FK relationship between JIB and JIBDETAIL (parallel views)
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_JIB') }}
),

renamed as (
    select
        -- identifiers
        ID::varchar as id,
        JIBIDENTITY::int as jib_identity,

        -- relationships
        AFEID::varchar as afe_id,
        APINVOICEID::varchar as ap_invoice_id,
        ARINVOICEID::varchar as ar_invoice_id,
        ACCOUNTID::varchar as account_id,
        COMPANYID::varchar as company_id,
        OWNERID::varchar as owner_id,
        VOUCHERID::varchar as voucher_id,
        WELLID::varchar as well_id,

        -- dates
        BILLEDDATE::date as billed_date,
        EFFECTIVEDATE::date as effective_date,
        EXPENSEDATE::date as expense_date,
        INVOICEDATE::date as invoice_date,

        -- denormalized codes/names
        trim(ACCOUNTSORT)::varchar as account_sort,
        trim(AFECODE)::varchar as afe_code,
        trim(AFTERCASEPOINT)::varchar as after_case_point,
        trim(APINVOICECODE)::varchar as ap_invoice_code,
        trim(ARINVOICECODE)::varchar as ar_invoice_code,
        trim(BILLINGSTATUS)::varchar as billing_status,
        trim(COMPANYCODE)::varchar as company_code,
        trim(COMPANYNAME)::varchar as company_name,
        trim(DESCRIPTION)::varchar as description,
        trim(ENTITYCODE)::varchar as entity_code,
        trim(ENTITYNAME)::varchar as entity_name,
        trim(ENTITYTYPE)::varchar as entity_type,
        trim(EXCHANGERATECODE)::varchar as exchange_rate_code,
        trim(EXPENSEDECKCODE)::varchar as expense_deck_code,
        trim(MAINACCOUNT)::varchar as main_account,
        trim(OWNERCODE)::varchar as owner_code,
        trim(OWNERNAME)::varchar as owner_name,
        trim(REFERENCEENTITYCODE)::varchar as reference_entity_code,
        trim(REFERENCEENTITYTYPE)::varchar as reference_entity_type,
        trim(SUBACCOUNT)::varchar as sub_account,
        trim(WELLCODE)::varchar as well_code,
        trim(WELLNAME)::varchar as well_name,

        -- financial
        EXPENSEDECKCHANGECODE::int as expense_deck_change_code,
        EXPENSEDECKINTEREST::float as expense_deck_interest,
        GROSSVALUE::float as gross_value,
        NETVALUE::float as net_value,
        VOUCHERCODE::int as voucher_code,

        -- suspense
        trim(OWNERSUSPENSE)::varchar as owner_suspense,
        trim(SUSPENSE)::varchar as suspense,
        trim(WELLSUSPENSE)::varchar as well_suspense,

        -- audit
        CREATEDATE::timestamp_ntz as created_at,
        UPDATEDATE::timestamp_ntz as updated_at,
        RECORDINSERTDATE::timestamp_ntz as record_inserted_at,
        RECORDUPDATEDATE::timestamp_ntz as record_updated_at,

        -- ingestion metadata
        "_meta/op"::varchar as _operation_type,
        FLOW_PUBLISHED_AT::timestamp_tz as _flow_published_at

    from source
),

filtered as (
    select *
    from renamed
    where
        _operation_type != 'd'
        and id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['id']) }} as jib_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        jib_sk,

        -- identifiers
        id,
        jib_identity,

        -- relationships
        afe_id,
        ap_invoice_id,
        ar_invoice_id,
        account_id,
        company_id,
        owner_id,
        voucher_id,
        well_id,

        -- dates
        billed_date,
        effective_date,
        expense_date,
        invoice_date,

        -- denormalized codes/names
        account_sort,
        afe_code,
        after_case_point,
        ap_invoice_code,
        ar_invoice_code,
        billing_status,
        company_code,
        company_name,
        description,
        entity_code,
        entity_name,
        entity_type,
        exchange_rate_code,
        expense_deck_code,
        main_account,
        owner_code,
        owner_name,
        reference_entity_code,
        reference_entity_type,
        sub_account,
        well_code,
        well_name,

        -- financial
        expense_deck_change_code,
        expense_deck_interest,
        gross_value,
        net_value,
        voucher_code,

        -- suspense
        owner_suspense,
        suspense,
        well_suspense,

        -- audit
        created_at,
        updated_at,
        record_inserted_at,
        record_updated_at,

        -- dbt metadata
        _loaded_at,

        -- ingestion metadata
        _operation_type,
        _flow_published_at

    from enhanced
)

select * from final
