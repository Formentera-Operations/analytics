{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA AR invoice adjustment detail records.

    Source: ODA_BATCH_ODA_ARINVOICEADJUSTMENTDETAIL (Estuary batch, 41K rows)
    Grain: One row per adjustment detail line (id)

    Notes:
    - Batch table — no CDC soft-delete filtering
    - Links adjustment details to adjustments, invoices, and AFEs
    - AFEID references unsynchronized table — no relationships test
    - NETVOLUME is NUMBER scale 0 — cast as int (not decimal)
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_BATCH_ODA_ARINVOICEADJUSTMENTDETAIL') }}
),

renamed as (
    select -- noqa: ST06
        -- identifiers
        trim(id)::varchar as id,
        arinvoiceadjustmentdetailidentity::int as ar_invoice_adjustment_detail_identity,

        -- relationships
        trim(invoiceadjustmentid)::varchar as invoice_adjustment_id,
        trim(invoiceid)::varchar as invoice_id,
        trim(afeid)::varchar as afe_id,

        -- financial
        adjustmentdetailamount::decimal(18, 2) as adjustment_detail_amount,

        -- volume
        netvolume::int as net_volume,

        -- descriptive
        description,

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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as arinvoiceadjustmentdetail_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        arinvoiceadjustmentdetail_sk,

        -- identifiers
        id,
        ar_invoice_adjustment_detail_identity,

        -- relationships
        invoice_adjustment_id,
        invoice_id,
        afe_id,

        -- financial
        adjustment_detail_amount,

        -- volume
        net_volume,

        -- descriptive
        description,

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
