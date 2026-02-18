{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'facts']
) }}

{#
    Fact: AR Aging Summary 
    
    -- Layer 5: Final AR Aging Report, ALl Transaction Lines (Invoices, Payments, Nettings, Adjustments) with reamining balance. 
    -- Grain: One row per invoice with posted/unposted balances 
    -- Dependencies: int_ar_all_details, int_ar_invoice_balances
    
    Sources:
    - int_oda_ar_all_details
    - stg_oda__voucher_v2
    - stg_oda__ar_invoice_type
  
#}

with ar_aging_detail as
(
    SELECT DISTINCT
      ar.company_code
    , ar.company_name
    , ar.owner_code
    , ar.owner_name
    , ar.invoice_number
    , ar.well_code
    , ar.well_name
    , ar.invoice_date                             AS transaction_date
    , REPLACE(ar.invoice_description, '   ', '')  AS transaction_reference
    , vou.code                                    AS voucher_number
    , ar.invoice_type                             AS transaction_type
    , ar.total_invoice_amount                     AS balance_due
    , ar.sort_order
    , irb.remaining_balance
    , CASE
          WHEN irb.remaining_balance <> 0
           AND irb.exclude_pair = 0
          THEN 1
          ELSE 0
      END                                         AS include_record
    , DATEDIFF(DAY, ar.invoice_date, GETDATE())   AS days_outstanding
    , CASE
          WHEN DATEDIFF(DAY, ar.invoice_date, GETDATE()) <= 0  THEN 'Current'
          WHEN DATEDIFF(DAY, ar.invoice_date, GETDATE()) <= 30 THEN '1-30'
          WHEN DATEDIFF(DAY, ar.invoice_date, GETDATE()) <= 60 THEN '31-60'
          WHEN DATEDIFF(DAY, ar.invoice_date, GETDATE()) <= 90 THEN '61-90'
          ELSE '91+'
      END                                         AS aging_bucket

        FROM {{ ref('int_oda_ar_all_details') }} AS ar

        LEFT JOIN {{ ref('stg_oda__voucher_v2') }} AS vou
            ON vou.id = ar.voucher_id

        LEFT JOIN {{ ref('int_oda_ar_invoice_remaining_balances') }} AS irb
            ON ar.invoice_id = irb.invoice_id
)
    Select * from ar_aging_detail