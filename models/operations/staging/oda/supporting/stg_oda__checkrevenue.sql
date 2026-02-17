{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Revenue Check Register

    Source: ODA_CHECKREVENUE (Estuary old connector, 659K rows)
    Grain: One row per revenue check (id)

    Notes:
    - OLD CONNECTOR: Table is from legacy Estuary connector (last altered 2026-01-11).
      Data may be stale — verify freshness before relying on recent records.
    - Integer booleans converted to true/false via coalesce(COL = 1, false)
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_CHECKREVENUE') }}
),

renamed as (
    select -- noqa: ST06
        -- identifiers
        ID::varchar as id,
        CHECKREVENUEIDENTITY::int as check_revenue_identity,
        ACCOUNTID::varchar as account_id,
        COMPANYID::varchar as company_id,
        CURRENCYID::varchar as currency_id,
        IMPORTDATAID::varchar as import_data_id,
        OWNERID::varchar as owner_id,
        PAYMENTTYPEID::int as payment_type_id,
        VOUCHERID::varchar as voucher_id,

        -- payment
        trim(PAYMENTTYPECODE)::varchar as payment_type_code,
        TRANSACTIONNUMBER::int as transaction_number,
        CHECKAMOUNT::float as check_amount,

        -- dates
        ACCRUALDATE::date as accrual_date,
        ISSUEDDATE::date as issued_date,
        ISSUEDDATEKEY::int as issued_date_key,
        GLVOIDDATE::date as gl_void_date,
        VOIDEDDATE::date as voided_date,
        VOID1099YEAR::int as void_1099_year,

        -- flags
        coalesce(INCLUDEINACCRUALREPORT = 1, false) as is_include_in_accrual_report,
        coalesce(RECONCILED = 1, false) as is_reconciled,
        coalesce(SYSTEMGENERATED = 1, false) as is_system_generated,
        coalesce(VOIDED = 1, false) as is_voided,

        -- audit
        CREATEDATE::timestamp_ntz as created_at,
        CREATEEVENTID::varchar as create_event_id,
        UPDATEDATE::timestamp_ntz as updated_at,
        UPDATEEVENTID::varchar as update_event_id,
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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as checkrevenue_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        checkrevenue_sk,

        -- identifiers
        id,
        check_revenue_identity,
        account_id,
        company_id,
        currency_id,
        import_data_id,
        owner_id,
        payment_type_id,
        voucher_id,

        -- payment
        payment_type_code,
        transaction_number,
        check_amount,

        -- dates
        accrual_date,
        issued_date,
        issued_date_key,
        gl_void_date,
        voided_date,
        void_1099_year,

        -- flags
        is_include_in_accrual_report,
        is_reconciled,
        is_system_generated,
        is_voided,

        -- audit
        created_at,
        create_event_id,
        updated_at,
        update_event_id,
        record_inserted_at,
        record_updated_at,

        -- dbt metadata
        _loaded_at,

        -- ingestion metadata
        _flow_published_at

    from enhanced
)

select * from final
