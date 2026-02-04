with source as (
    select * from {{ source('oda', 'ODA_ARINVOICEPAYMENT') }}
),

renamed as (
    select
    -- IDs
    ID                              AS id,
    ARINVOICEPAYMENTIDENTITY        AS ar_invoice_payment_identity,
    VOUCHERID                       AS voucher_id,
    CASHACCOUNTID                   AS cash_account_id,
    OWNERID                         AS owner_id, 
    PAYMENTTYPEID                    AS payment_type_id,

    
    -- Numerics
    PAYMENTAMOUNT                   AS payment_amount, 

    -- Dates
    ACCRUALDATE                     AS accrual_date,
    PAYMENTDATE                     AS payment_date,

    -- Booleans
    POSTED                          AS posted,

   -- Strings
    DESCRIPTION                     AS description,
    PAYMENTTYPECODE                 AS payment_type_code,
    PAYMENTTYPENAME                 AS payment_type_name,

    -- Timestamps
    CREATEDATE                      AS create_date,
    UPDATEDATE                      AS update_date,
    RECORDINSERTDATE                AS record_insert_date,
    RECORDUPDATEDATE                AS record_update_date,

    -- Metadata
    FLOW_PUBLISHED_AT               AS flow_published_at,
    FLOW_DOCUMENT                   AS flow_document

 
    from source
)

select * from renamed