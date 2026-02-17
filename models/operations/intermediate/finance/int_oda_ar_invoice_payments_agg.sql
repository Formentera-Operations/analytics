{{ config(
    enabled=true,
    materialized='view'
) }}

{#
    Dimension: Total payments per invoice
    
    -- Layer 2: Invoice Payments Aggregation
    -- Purpose: Calculate total payments per invoice
    -- Dependencies: int_ar_payments
    
    Sources:
    - int_oda_ar_payments
#}

    with ar_payments_agg as (
        SELECT
      d.invoice_id
    , v.posted                            AS voucher_posted
    , COALESCE(SUM(d.amount_applied), 0.00) AS total_payments_applied

        FROM {{ref('stg_oda__arinvoicepaymentdetail')}} AS d

        INNER JOIN {{ref('stg_oda__arinvoicepayment') }} AS p
        ON p.id = d.invoice_payment_id

         JOIN {{ ref('stg_oda__voucher_v2') }} AS v
         ON v.id = p.voucher_id

GROUP BY
      d.invoice_id
    , v.posted
    )
        select * from ar_payments_agg
