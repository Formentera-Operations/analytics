{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Accounts Receivable invoice headers.

    Source: ODA_ARINVOICE_V2 (Estuary CDC, 436K rows)
    Grain: One row per AR invoice (id)

    Notes:
    - Soft deletes filtered via _operation_type != 'd'
    - All boolean columns are native BOOLEAN type (not integer flags)
#}

with

source as (
    select * from {{ source('oda', 'ODA_ARINVOICE_V2') }}
),

renamed as (  -- noqa: ST06
    select
        -- identifiers
        ID::varchar as id,
        ARINVOICE_V2IDENTITY::int as arinvoice_v2_identity,
        OWNERID::varchar as owner_id,
        COMPANYID::varchar as company_id,
        WELLID::varchar as well_id,
        VOUCHERID::varchar as voucher_id,
        INVOICETYPEID::int as invoice_type_id,
        PAYMENTSTATUSID::int as payment_status_id,
        STATEMENTSTATUSID::int as statement_status_id,
        ADJUSTMENTID::varchar as adjustment_id,
        ADVANCEINVOICEID::varchar as advance_invoice_id,
        ADVANCEPOSTINGEXCHANGERATEID::varchar as advance_posting_exchange_rate_id,
        GAINLOSSEXCHANGERATEID::varchar as gain_loss_exchange_rate_id,
        IMPORTDATAID::varchar as import_data_id,
        PAYMENTEXCHANGERATEID::varchar as payment_exchange_rate_id,
        POSTINGEXCHANGERATEID::varchar as posting_exchange_rate_id,
        CREATEEVENTID::varchar as create_event_id,
        UPDATEEVENTID::varchar as update_event_id,

        -- dates
        INVOICEDATE::date as invoice_date,
        INVOICEDATEKEY::float as invoice_date_key,
        ACCRUALDATE::date as accrual_date,
        ADVANCEINVOICEDATE::date as advance_invoice_date,
        GAINLOSSPOSTEDTHRUDATE::date as gain_loss_posted_thru_date,

        -- descriptive
        trim(CODE)::varchar as code,
        trim(CODESORT)::varchar as code_sort,
        trim(DESCRIPTION)::varchar as description,

        -- financial
        coalesce(INVOICEAMOUNT, 0)::decimal(18, 2) as invoice_amount,

        -- flags
        POSTED::boolean as is_posted,
        FULLYPAID::boolean as is_fully_paid,
        INCLUDEINACCRUALREPORT::boolean as is_include_in_accrual_report,
        ISOVERAGEINVOICE::boolean as is_overage_invoice,
        PRINTPENDING::boolean as is_print_pending,

        -- audit
        CREATEDATE::timestamp_ntz as created_at,
        UPDATEDATE::timestamp_ntz as updated_at,
        RECORDINSERTDATE::timestamp_ntz as record_inserted_at,
        RECORDUPDATEDATE::timestamp_ntz as record_updated_at,

        -- ingestion metadata
        "_meta/op"::varchar as _operation_type,
        FLOW_PUBLISHED_AT::timestamp_ntz as _flow_published_at

        -- FLOW_DOCUMENT excluded

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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as arinvoice_v2_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        arinvoice_v2_sk,

        -- identifiers
        id,
        arinvoice_v2_identity,
        owner_id,
        company_id,
        well_id,
        voucher_id,
        invoice_type_id,
        payment_status_id,
        statement_status_id,
        adjustment_id,
        advance_invoice_id,
        advance_posting_exchange_rate_id,
        gain_loss_exchange_rate_id,
        import_data_id,
        payment_exchange_rate_id,
        posting_exchange_rate_id,
        create_event_id,
        update_event_id,

        -- dates
        invoice_date,
        invoice_date_key,
        accrual_date,
        advance_invoice_date,
        gain_loss_posted_thru_date,

        -- descriptive
        code,
        code_sort,
        description,

        -- financial
        invoice_amount,

        -- flags
        is_posted,
        is_fully_paid,
        is_include_in_accrual_report,
        is_overage_invoice,
        is_print_pending,

        -- audit
        created_at,
        updated_at,
        record_inserted_at,
        record_updated_at,

        -- ingestion metadata
        _operation_type,
        _flow_published_at,
        _loaded_at

    from enhanced
)

select * from final
