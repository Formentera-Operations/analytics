{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA AR invoice payment line details.

    Source: ODA_ARINVOICEPAYMENTDETAIL (Estuary batch, 172K rows)
    Grain: One row per payment-to-invoice application (id)

    Notes:
    - Batch table — no CDC soft delete filtering needed
    - Links payments to invoices with amount applied
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_ARINVOICEPAYMENTDETAIL') }}
),

renamed as (
    select -- noqa: ST06
        -- identifiers
        ID::varchar as id,
        ARINVOICEPAYMENTDETAILIDENTITY::int as arinvoicepaymentdetail_identity,
        trim(INVOICEID)::varchar as invoice_id,
        trim(INVOICEPAYMENTID)::varchar as invoice_payment_id,

        -- financial
        coalesce(AMOUNTAPPLIED, 0)::decimal(18, 2) as amount_applied,
        coalesce(AMOUNTDUEBEFOREPOSTING, 0)::decimal(18, 2) as amount_due_before_posting,

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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as arinvoicepaymentdetail_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        arinvoicepaymentdetail_sk,

        -- identifiers
        id,
        arinvoicepaymentdetail_identity,
        invoice_id,
        invoice_payment_id,

        -- financial
        amount_applied,
        amount_due_before_posting,

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
