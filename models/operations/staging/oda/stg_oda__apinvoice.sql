with source as (
    select * from {{ source('oda', 'APINVOICE') }}
),

renamed as (
    select
        -- ids
        id,
        apinvoiceidentity as ap_invoice_identity,
        cashaccountid as cash_account_id,
        companyid as company_id,
        createeventid as create_event_id,
        currencyid as currency_id,
        importdataid as import_data_id,
        originalinvoiceid as original_invoice_id,
        paymentexchangerateid as payment_exchange_rate_id,
        postingexchangerateid as posting_exchange_rate_id,
        updateeventid as update_event_id,
        vendorid as vendor_id,
        voucherid as voucher_id,

        -- dates
        acceptancedate as acceptance_date,
        accrualdate as accrual_date,
        discountcutoffdate as discount_cutoff_date,
        duedate as due_date,
        invoicedate as invoice_date,
        manualtransactiondate as manual_transaction_date,
        receiveddate as received_date,

        -- numerics
        amountdue as amount_due,
        checkstubcount as check_stub_count,
        discountallowed as discount_allowed,
        discountpercentage as discount_percentage,
        discounttakenamount as discount_taken_amount,
        invoiceamount as invoice_amount,
        invoicedatekey as invoice_date_key,
        manualprepayment as manual_prepayment,
        manualtransactionnumber as manual_transaction_number,
        manualtransactiontypeid as manual_transaction_type_id,
        nid as n_id,
        paidamount as paid_amount,
        partialpaymentpending as partial_payment_pending,
        paymenttypeid as payment_type_id,
        tobepaid as to_be_paid,

        -- strings
        code,
        codesort as code_sort,
        description,
        externalid as external_id,
        invoiceexternalid as invoice_external_id,
        paymentstatuscode as payment_status_code,
        paymenttypecode as payment_type_code,

        -- booleans
        approvedforposting as approved_for_posting,
        isfromaphistoryimport as is_from_ap_history_import,
        paymentdetailincomplete as payment_detail_incomplete,
        posted,
        readytopay as ready_to_pay,

        -- timestamps
        createdate as created_at,
        recordinsertdate as record_inserted_at,
        recordupdatedate as record_updated_at,
        updatedate as updated_at,

        -- metadata
       --_meta_op,
        flow_published_at,
        flow_document

    from source
)

select * from renamed