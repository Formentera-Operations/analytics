with source as (
    select * from {{ source('oda', 'ODA_ARINVOICE') }}
),

renamed as (
    select
        -- ids
        ID                      AS ar_id,
        ARINVOICEIDENTITY       AS ar_invoice_identity,
        OWNERID                 AS owner_id,
        PAYMENTSTATUSID         AS payment_status_id,
        VOUCHERID               AS voucher_id,
        WELLID                  AS well_id,
        CODE                    AS invoice_code,
        COMPANYID               AS company_id,
        
        -- Dates
        ACCRUALDATE             AS accrual_date,
        INVOICEDATE             AS invoice_date,
        INVOICEDATEKEY          AS invoice_date_key,
        UNPOSTEDBALANCEDUE      AS unposted_balance_due,
        
        --numerics
        INVOICEAMOUNT           AS invoice_amount,
        POSTEDBALANCEDUE        AS posted_balance_due,
        PAYMENTSTATUSCODE       AS payment_status_code,

        --strings
        DESCRIPTION             AS description,
        TRANSACTIONTYPE         AS transaction_type,

        --booleans
        POSTED                  AS posted,
        
        -- timestamps
        CREATEDATE as created_at,
        RECORDINSERTDATE as record_inserted_at,
        RECORDUPDATEDATE as record_updated_at,
        UPDATEDATE as updated_at,

        -- metadata
       --_meta_op,
        flow_published_at,
        flow_document

    from source
)

select * from renamed