{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Accounts Payable Invoices

    Source: ODA_APINVOICE (Estuary CDC)
    Grain: One row per AP invoice (id)

    Notes:
    - Soft deletes filtered out (operation_type = 'd')
    - Financial values cast to float
    - Integer booleans converted to true/false via coalesce(COL = 1, false)
#}

with

source as (
    select * from {{ source('oda', 'ODA_APINVOICE') }}
),

renamed as (
    select
        -- identifiers
        ID::varchar as id,
        APINVOICEIDENTITY::int as ap_invoice_identity,
        COMPANYID::varchar as company_id,
        VENDORID::varchar as vendor_id,
        VOUCHERID::varchar as voucher_id,
        CURRENCYID::varchar as currency_id,
        CASHACCOUNTID::varchar as cash_account_id,
        ORIGINALINVOICEID::varchar as original_invoice_id,
        IMPORTDATAID::varchar as import_data_id,
        CREATEEVENTID::varchar as create_event_id,
        UPDATEEVENTID::varchar as update_event_id,
        NID::int as n_id,

        -- invoice details
        trim(CODE)::varchar as code,
        trim(CODESORT)::varchar as code_sort,
        trim(DESCRIPTION)::varchar as description,
        trim(EXTERNALID)::varchar as external_id,
        trim(INVOICEEXTERNALID)::varchar as invoice_external_id,

        -- dates
        ACCEPTANCEDATE::date as acceptance_date,
        ACCRUALDATE::date as accrual_date,
        DISCOUNTCUTOFFDATE::date as discount_cutoff_date,
        DUEDATE::date as due_date,
        INVOICEDATE::date as invoice_date,
        INVOICEDATEKEY::int as invoice_date_key,
        MANUALTRANSACTIONDATE::date as manual_transaction_date,
        RECEIVEDDATE::date as received_date,

        -- financial
        AMOUNTDUE::float as amount_due,
        DISCOUNTALLOWED::float as discount_allowed,
        DISCOUNTPERCENTAGE::float as discount_percentage,
        DISCOUNTTAKENAMOUNT::float as discount_taken_amount,
        INVOICEAMOUNT::float as invoice_amount,
        MANUALPREPAYMENT::float as manual_prepayment,
        PAIDAMOUNT::float as paid_amount,
        PARTIALPAYMENTPENDING::float as partial_payment_pending,
        TOBEPAID::float as to_be_paid,

        -- payment status
        trim(PAYMENTSTATUSCODE)::varchar as payment_status_code,
        PAYMENTSTATUSID::int as payment_status_id,
        trim(PAYMENTTYPECODE)::varchar as payment_type_code,
        PAYMENTTYPEID::int as payment_type_id,
        MANUALTRANSACTIONNUMBER::int as manual_transaction_number,
        MANUALTRANSACTIONTYPEID::int as manual_transaction_type_id,
        CHECKSTUBCOUNT::int as check_stub_count,

        -- exchange rate
        GAINLOSSEXCHANGERATEID::varchar as gain_loss_exchange_rate_id,
        GAINLOSSPOSTEDTHRUDATE::date as gain_loss_posted_thru_date,
        PAYMENTEXCHANGERATEID::varchar as payment_exchange_rate_id,
        POSTINGEXCHANGERATEID::varchar as posting_exchange_rate_id,

        -- flags
        CREATEDATE::timestamp_ntz as created_at,
        UPDATEDATE::timestamp_ntz as updated_at,
        RECORDINSERTDATE::timestamp_ntz as record_inserted_at,
        RECORDUPDATEDATE::timestamp_ntz as record_updated_at,
        "_meta/op"::varchar as _operation_type,
        FLOW_PUBLISHED_AT::timestamp_tz as _flow_published_at,

        -- audit
        coalesce(APPROVEDFORPOSTING = 1, false) as is_approved_for_posting,
        coalesce(INCLUDEINACCRUALREPORT = 1, false) as is_include_in_accrual_report,
        coalesce(ISFROMAPHISTORYIMPORT = 1, false) as is_from_ap_history_import,
        coalesce(PAYMENTDETAILINCOMPLETE = 1, false) as is_payment_detail_incomplete,

        -- ingestion metadata
        coalesce(POSTED = 1, false) as is_posted,
        coalesce(READYTOPAY = 1, false) as is_ready_to_pay

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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as apinvoice_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        apinvoice_sk,

        -- identifiers
        id,
        ap_invoice_identity,
        company_id,
        vendor_id,
        voucher_id,
        currency_id,
        cash_account_id,
        original_invoice_id,
        import_data_id,
        create_event_id,
        update_event_id,
        n_id,

        -- invoice details
        code,
        code_sort,
        description,
        external_id,
        invoice_external_id,

        -- dates
        acceptance_date,
        accrual_date,
        discount_cutoff_date,
        due_date,
        invoice_date,
        invoice_date_key,
        manual_transaction_date,
        received_date,

        -- financial
        amount_due,
        discount_allowed,
        discount_percentage,
        discount_taken_amount,
        invoice_amount,
        manual_prepayment,
        paid_amount,
        partial_payment_pending,
        to_be_paid,

        -- payment status
        payment_status_code,
        payment_status_id,
        payment_type_code,
        payment_type_id,
        manual_transaction_number,
        manual_transaction_type_id,
        check_stub_count,

        -- exchange rate
        gain_loss_exchange_rate_id,
        gain_loss_posted_thru_date,
        payment_exchange_rate_id,
        posting_exchange_rate_id,

        -- flags
        is_approved_for_posting,
        is_include_in_accrual_report,
        is_from_ap_history_import,
        is_payment_detail_incomplete,
        is_posted,
        is_ready_to_pay,

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
