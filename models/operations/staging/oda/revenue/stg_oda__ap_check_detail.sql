{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA AP Check Detail.

    Source: ODA_APCHECKDETAIL (Estuary batch, ~324K rows)
    Grain: One row per AP check-to-invoice payment allocation (id)

    Notes:
    - Accounts payable domain — staged in revenue/ directory for M2 sprint scope.
      If an ap/ directory is created later, this model can be moved.
    - Batch table — no CDC soft delete filtering needed (_meta/op is always 'c')
    - No deduplication needed — Estuary batch handles dedup at the connector level
    - VOIDED is native BOOLEAN type in information_schema
    - Timestamps are TIMESTAMP_LTZ in source → cast to ::timestamp_ntz per convention
    - No CREATEEVENTID/UPDATEEVENTID — simpler audit set than other ODA tables
    - _meta/op excluded — batch table, never contains 'd' operations
    - FLOW_DOCUMENT excluded — large JSON blob, not needed downstream
    - Validated against information_schema.columns on 2026-02-20
#}

with

source as (
    select * from {{ source('oda', 'ODA_APCHECKDETAIL') }}
),

renamed as (
    select
        -- identifiers
        ID::varchar as id,
        APCHECKDETAILIDENTITY::int as ap_check_detail_identity,
        CHECKID::varchar as check_id,
        INVOICEID::varchar as invoice_id,
        COMPANYID::varchar as company_id,
        VENDORID::varchar as vendor_id,
        VOUCHERID::varchar as voucher_id,

        -- financial
        PAYMENTAMOUNT::float as payment_amount,
        DISCOUNTAMOUNT::float as discount_amount,
        FEDERALWITHHOLDING::float as federal_withholding,
        STATEWITHHOLDING::float as state_withholding,

        -- flags
        VOIDED::boolean as is_voided,

        -- foreign currency
        GAINLOSSEXCHANGERATEID::varchar as gain_loss_exchange_rate_id,
        GAINLOSSPOSTEDTHRUDATE::timestamp_ntz as gain_loss_posted_thru_date,

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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as ap_check_detail_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        ap_check_detail_sk,

        -- identifiers
        id,
        ap_check_detail_identity,
        check_id,
        invoice_id,
        company_id,
        vendor_id,
        voucher_id,

        -- financial
        payment_amount,
        discount_amount,
        federal_withholding,
        state_withholding,

        -- flags
        is_voided,

        -- foreign currency
        gain_loss_exchange_rate_id,
        gain_loss_posted_thru_date,

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
