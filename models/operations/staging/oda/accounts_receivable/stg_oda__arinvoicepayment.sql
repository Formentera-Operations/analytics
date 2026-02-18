{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA AR invoice payment headers.

    Source: ODA_ARINVOICEPAYMENT (Estuary batch, 37.6K rows)
    Grain: One row per AR payment (id)

    Notes:
    - Batch table — no CDC soft delete filtering needed
    - POSTED is native BOOLEAN type
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_ARINVOICEPAYMENT') }}
),

renamed as (
    select -- noqa: ST06
        -- identifiers
        ID::varchar as id,
        ARINVOICEPAYMENTIDENTITY::int as arinvoicepayment_identity,
        trim(VOUCHERID)::varchar as voucher_id,
        trim(OWNERID)::varchar as owner_id,
        trim(CASHACCOUNTID)::varchar as cash_account_id,
        PAYMENTTYPEID::int as payment_type_id,

        -- dates
        ACCRUALDATE::date as accrual_date,
        PAYMENTDATE::date as payment_date,

        -- descriptive
        trim(DESCRIPTION)::varchar as description,
        trim(PAYEECHECKNUMBER)::varchar as payee_check_number,
        trim(PAYMENTTYPECODE)::varchar as payment_type_code,
        trim(PAYMENTTYPENAME)::varchar as payment_type_name,

        -- financial
        coalesce(PAYMENTAMOUNT, 0)::decimal(18, 2) as payment_amount,

        -- flags
        POSTED::boolean as is_posted,

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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as arinvoicepayment_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        arinvoicepayment_sk,

        -- identifiers
        id,
        arinvoicepayment_identity,
        voucher_id,
        owner_id,
        cash_account_id,
        payment_type_id,

        -- dates
        accrual_date,
        payment_date,

        -- descriptive
        description,
        payee_check_number,
        payment_type_code,
        payment_type_name,

        -- financial
        payment_amount,

        -- flags
        is_posted,

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
