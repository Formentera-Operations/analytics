{{ config(
    enabled=true,
    materialized='view'
) }}

{#
    Dimension: Company AR Invoice Payment Summary
    Payment Transactions applied to invoices
    
    -- Layer 1: Payment Details Model
    -- Purpose: Extract and standardize payment transaction data
    -- Dependencies: Base tables only
    
    Sources:
    - stg_oda__arinvoicepayment
    - stg_oda__arinvoicepaymentdetail
    - stg_oda__arinvoice_v2
    - stg_oda__voucher_v2
    - stg_oda__company_v2
    - stg_oda__owner_v2
    - stg_oda__entity_v2
    - stg_oda__wells
#}
with ar_payments as
(
    
  SELECT
      i.company_code
    , i.company_name
    , i.owner_id
    , i.owner_code
    , i.owner_name
    , i.well_id
    , i.well_code
    , i.well_name
    , i.invoice_number
    , i.invoice_id
    , i.invoice_type_id
    , i.hold_billing
    , p.voucher_id
    , CONCAT('Payment, Check# ', p.payee_check_number)
                                          AS invoice_description
    , 'Pymt'                              AS invoice_type
    , p.payment_date                      AS invoice_date
    , pd.amount_applied                   AS total_invoice_amount
    , 2                                   AS sort_order

    FROM {{ ref('stg_oda__arinvoicepaymentdetail') }} AS pd

    INNER JOIN {{ ref('stg_oda__arinvoicepayment') }} AS p
        ON p.id = pd.invoice_payment_id

    INNER JOIN {{ ref('stg_oda__voucher_v2')}} AS v
        ON v.id = p.voucher_id

    INNER JOIN {{ ref('int_oda_ar_invoice') }} AS i
        ON i.invoice_id = pd.invoice_id

    WHERE i.voucher_posted = 1
    AND v.posted         = 1

    )
        select * from ar_payments