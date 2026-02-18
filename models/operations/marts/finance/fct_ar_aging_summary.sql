{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'facts']
) }}

{#
    Fact: AR Aging Summary 
    
    -- Layer 5: Final AR Aging Report
    -- Grain: One row per invoice with posted/unposted balances 
    -- Dependencies: int_ar_all_details, int_ar_invoice_balances
    
    Sources:
    - int_oda_ar_invoice
    - int_oda_ar_invoice_balances
  
#}

with ar_aging_summary as 
(
    SELECT
      i.invoice_id
    , i.invoice_number
    , i.voucher_id
    , i.voucher_code
    , i.invoice_amount
    , i.invoice_date
    , i.owner_id
    , i.owner_code
    , i.owner_name
    , i.company_id
    , i.company_code
    , i.company_name
    , i.well_id
    , i.well_code
    , i.well_name
    , i.invoice_type_id
    , i.invoice_type
    , i.description
    , i.voucher_posted
    , bal.posted_balance_due
    , bal.unposted_balance_due
    , DATEDIFF(DAY, i.invoice_date, GETDATE())
                                          AS days_outstanding
    , CASE
          WHEN DATEDIFF(DAY, i.invoice_date, GETDATE()) <= 0
              THEN bal.posted_balance_due
          ELSE 0.00
      END                                 AS aging_current
    , CASE
          WHEN DATEDIFF(DAY, i.invoice_date, GETDATE()) BETWEEN 1 AND 30
              THEN bal.posted_balance_due
          ELSE 0.00
      END                                 AS aging_1_to_30
    , CASE
          WHEN DATEDIFF(DAY, i.invoice_date, GETDATE()) BETWEEN 31 AND 60
              THEN bal.posted_balance_due
          ELSE 0.00
      END                                 AS aging_31_to_60
    , CASE
          WHEN DATEDIFF(DAY, i.invoice_date, GETDATE()) BETWEEN 61 AND 90
              THEN bal.posted_balance_due
          ELSE 0.00
      END                                 AS aging_61_to_90
    , CASE
          WHEN DATEDIFF(DAY, i.invoice_date, GETDATE()) > 90
              THEN bal.posted_balance_due
          ELSE 0.00
      END                                 AS aging_91_plus
    , CASE
          WHEN DATEDIFF(DAY, i.invoice_date, GETDATE()) <= 0  THEN 'Current'
          WHEN DATEDIFF(DAY, i.invoice_date, GETDATE()) <= 30 THEN '1-30'
          WHEN DATEDIFF(DAY, i.invoice_date, GETDATE()) <= 60 THEN '31-60'
          WHEN DATEDIFF(DAY, i.invoice_date, GETDATE()) <= 90 THEN '61-90'
          ELSE '91+'
      END                                 AS aging_bucket

        FROM {{ ref('int_oda_ar_invoice') }} AS i

        LEFT JOIN {{ ref('int_oda_ar_invoice_balances') }} AS bal
            ON i.invoice_id = bal.invoice_id
    
)
    Select * from ar_aging_summary
