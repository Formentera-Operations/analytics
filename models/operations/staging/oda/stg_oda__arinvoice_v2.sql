with source as (
    select * from {{ source('oda', 'ODA_ARINVOICE_V2') }}
),

renamed as (
    select
           -- IDs
    ID                              AS id,
    ARINVOICE_V2IDENTITY            AS ar_invoice_v2_identity,
    ADJUSTMENTID                    AS adjustment_id,
    ADVANCEINVOICEID                AS advance_invoice_id,
    ADVANCEPOSTINGEXCHANGERATEID    AS advance_posting_exchange_rate_id,
    COMPANYID                       AS company_id,
    CODE                            AS code,
    GAINLOSSEXCHANGERATEID          AS gain_loss_exchange_rate_id,
    IMPORTDATAID                    AS import_data_id,
    INVOICETYPEID                   AS invoice_type_id,
    OWNERID                         AS owner_id,
    PAYMENTEXCHANGERATEID           AS payment_exchange_rate_id,
    PAYMENTSTATUSID                 AS payment_status_id,
    POSTINGEXCHANGERATEID           AS posting_exchange_rate_id,
    STATEMENTSTATUSID               AS statement_status_id,
    VOUCHERID                       AS voucher_id,
    WELLID                          AS well_id,
    CREATEEVENTID                   AS create_event_id,
    UPDATEEVENTID                   AS update_event_id,

    -- Dates
    ACCRUALDATE                     AS accrual_date,
    ADVANCEINVOICEDATE              AS advance_invoice_date,
    GAINLOSSPOSTEDTHRUDATE          AS gain_loss_posted_thru_date,
    INVOICEDATE                     AS invoice_date,
    INVOICEDATEKEY                  AS invoice_date_key,

    -- Numeric Values
    INVOICEAMOUNT                   AS invoice_amount,

    -- Strings
    CODESORT                        AS code_sort,
    DESCRIPTION                     AS description,

    -- Booleans
    FULLYPAID                       AS fully_paid,
    INCLUDEINACCRUALREPORT          AS include_in_accrual_report,
    ISOVERAGEINVOICE                AS is_overage_invoice,
    POSTED                          AS posted,
    PRINTPENDING                    AS print_pending,

    -- Timestamps
    CREATEDATE                      AS create_date,
    UPDATEDATE                      AS update_date,
    RECORDINSERTDATE                AS record_insert_date,
    RECORDUPDATEDATE                AS record_update_date,

    -- Metadata
    "_meta/op"                      AS _meta_op,
    FLOW_PUBLISHED_AT               AS flow_published_at,
    FLOW_DOCUMENT                   AS flow_document

    from source
)

select * from renamed