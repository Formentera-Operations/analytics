{{ config(
    enabled=true,
    materialized='view'
) }}

{#
    Dimension: Calculates remaining balance per invoice considering all transaction types
    
    -- Layer 3: Invoice Remaining Balances
    -- Purpose: Calculate Posted and Unposted Balance Due per invoice
    -- Dependencies: int_ar_invoices, int_ar_invoice_payments_agg, int_ar_invoice_adjustments_agg, int_ar_invoice_netting_agg
    
    Sources:
    - int_oda_ar_invoice
    - int_ar_invoice_payments_agg
    - int_ar_invoice_adjustments_agg
    - int_ar_invoice_netting_agg
#}

with ar_invoice_balances as (
   SELECT
        i.invoice_id
        , i.invoice_amount
        , CAST(
            i.invoice_amount
            + COALESCE(pp.total_payments_applied, 0.00)
            + COALESCE(pa.total_adjustments, 0.00)
            - COALESCE(n.total_netted, 0.00)
        AS DECIMAL(19, 4))                  AS posted_balance_due
        , CAST(
            i.invoice_amount
            + COALESCE(up.total_payments_applied, 0.00)
            + COALESCE(ua.total_adjustments, 0.00)
            - COALESCE(n.total_netted, 0.00)
        AS DECIMAL(19, 4))                  AS unposted_balance_due

        FROM {{ ref('int_oda_ar_invoice') }} AS i

        LEFT JOIN {{ ref('int_oda_ar_invoice_payments_agg') }} AS pp
            ON  i.invoice_id = pp.invoice_id
            AND pp.voucher_posted = 1

        LEFT JOIN {{ ref('int_oda_ar_invoice_payments_agg') }} AS up
            ON  i.invoice_id = up.invoice_id
            AND up.voucher_posted = 0

        LEFT JOIN {{ ref('int_oda_ar_invoice_adjustments_agg') }} AS pa
            ON  i.invoice_id = pa.invoice_id
            AND pa.voucher_posted = 1

        LEFT JOIN {{ ref('int_oda_ar_invoice_adjustments_agg') }} AS ua
            ON  i.invoice_id = ua.invoice_id
            AND ua.voucher_posted = 0

        LEFT JOIN {{ ref('int_oda_ar_invoice_netting_agg') }} AS n
            ON  i.invoice_id = n.invoice_id
    )
        select * from ar_invoice_balances