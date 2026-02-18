{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA AR invoice adjustment records.

    Source: ODA_BATCH_ODA_ARINVOICEADJUSTMENT (Estuary batch, 10.7K rows)
    Grain: One row per invoice adjustment (id)

    Notes:
    - Batch table — no CDC soft-delete filtering
    - Links adjustments to accounts, companies, owners, and vouchers
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_BATCH_ODA_ARINVOICEADJUSTMENT') }}
),

renamed as (
    select -- noqa: ST06
        -- identifiers
        trim(id)::varchar as id,
        arinvoiceadjustmentidentity::int as ar_invoice_adjustment_identity,

        -- relationships
        trim(accountid)::varchar as account_id,
        trim(companyid)::varchar as company_id,
        trim(ownerid)::varchar as owner_id,
        trim(voucherid)::varchar as voucher_id,
        adjustmenttypeid::int as adjustment_type_id,

        -- dates
        accrualdate::date as accrual_date,
        adjustmentdate::timestamp_ntz as adjustment_date,

        -- descriptive
        description,

        -- financial
        adjustmentamount::decimal(18, 2) as adjustment_amount,

        -- flags
        includeinaccrualreport::boolean as is_include_in_accrual_report,
        posted::boolean as is_posted,

        -- audit
        createdate::timestamp_ntz as created_at,
        trim(createeventid)::varchar as create_event_id,
        updatedate::timestamp_ntz as updated_at,
        trim(updateeventid)::varchar as update_event_id,
        recordinsertdate::timestamp_ntz as record_inserted_at,
        recordupdatedate::timestamp_ntz as record_updated_at,

        -- ingestion metadata
        flow_published_at::timestamp_tz as _flow_published_at

    from source
),

filtered as (
    select *
    from renamed
    where id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['id']) }} as arinvoiceadjustment_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        arinvoiceadjustment_sk,

        -- identifiers
        id,
        ar_invoice_adjustment_identity,

        -- relationships
        account_id,
        company_id,
        owner_id,
        voucher_id,
        adjustment_type_id,

        -- dates
        accrual_date,
        adjustment_date,

        -- descriptive
        description,

        -- financial
        adjustment_amount,

        -- flags
        is_include_in_accrual_report,
        is_posted,

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
