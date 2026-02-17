{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Accounts Payable Checks

    Source: ODA_APCHECK (Estuary old connector, batch-like)
    Grain: One row per AP check (id)

    Notes:
    - NOT a CDC table — no soft-delete filtering needed
    - Integer booleans converted to true/false via coalesce(COL = 1, false)
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_APCHECK') }}
),

renamed as (
    select -- noqa: ST06
        -- identifiers
        ID::varchar as id,
        APCHECKIDENTITY::int as ap_check_identity,
        COMPANYID::varchar as company_id,
        VENDORID::varchar as vendor_id,
        ACCOUNTID::varchar as account_id,
        VOUCHERID::varchar as voucher_id,
        TRANSACTIONNUMBER::int as transaction_number,

        -- payment
        PAYMENTAMOUNT::float as payment_amount,
        trim(PAYMENTTYPECODE)::varchar as payment_type_code,
        PAYMENTTYPEID::int as payment_type_id,

        -- dates
        ACCRUALDATE::date as accrual_date,
        ISSUEDDATE::date as issued_date,
        ISSUEDDATEKEY::int as issued_date_key,

        -- flags
        coalesce(RECONCILED = 1, false) as is_reconciled,
        coalesce(SYSTEMGENERATED = 1, false) as is_system_generated,
        coalesce(VOIDED = 1, false) as is_voided,
        VOIDEDDATE::date as voided_date,
        VOID1099YEAR::int as void_1099_year,

        -- audit
        CREATEDATE::timestamp_ntz as created_at,
        UPDATEDATE::timestamp_ntz as updated_at,
        RECORDINSERTDATE::timestamp_ntz as record_inserted_at,
        RECORDUPDATEDATE::timestamp_ntz as record_updated_at,

        -- ingestion metadata
        FLOW_PUBLISHED_AT::timestamp_tz as _flow_published_at

    from source
),

filtered as (
    select *
    from renamed
    where id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['id']) }} as apcheck_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        apcheck_sk,

        -- identifiers
        id,
        ap_check_identity,
        company_id,
        vendor_id,
        account_id,
        voucher_id,
        transaction_number,

        -- payment
        payment_amount,
        payment_type_code,
        payment_type_id,

        -- dates
        accrual_date,
        issued_date,
        issued_date_key,

        -- flags
        is_reconciled,
        is_system_generated,
        is_voided,
        voided_date,
        void_1099_year,

        -- audit
        created_at,
        updated_at,
        record_inserted_at,
        record_updated_at,

        -- dbt metadata
        _loaded_at,

        -- ingestion metadata
        _flow_published_at

    from enhanced
)

select * from final
