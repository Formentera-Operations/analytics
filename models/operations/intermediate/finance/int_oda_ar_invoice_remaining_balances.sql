{{ config(
    enabled=true,
    materialized='view'
) }}

{#
    Dimension: Remaining balance per invoice (invoice + payments + adjustments + nettings) with an exclude flag for advance-closeout pairs.
  
    
    -- Grain: One Row per AR Invoice ID
    -- Purpose: Extract and standardize AR invoice data
    -- Dependencies: Base tables only
    
    Sources:
    - int_oda_ar_invoice
    - int_oda_ar_invoice_payments_agg
    - int_oda_ar_invoice_adjustments_agg
    - int_oda_ar_invoice_netting_agg
    - int_oda_ar_advance_closeout_pairs
#}
 WITH ar_remaining_balance AS (         
    WITH excluded_invoices AS (

            SELECT
                advance_invoice_id              AS invoice_id
                , 1                               AS exclude_pair
            FROM {{ ref('int_oda_ar_advance_closeout_pairs') }}

            UNION ALL

            SELECT
                closeout_invoice_id             AS invoice_id
                , 1                               AS exclude_pair
            FROM {{ ref('int_oda_ar_advance_closeout_pairs') }}

        )

        SELECT
            i.invoice_id
            , i.invoice_amount
                + COALESCE(pp.total_payments_applied, 0.00)
                + COALESCE(pa.total_adjustments, 0.00)
                - COALESCE(n.total_netted, 0.00)
                                                AS remaining_balance
            , COALESCE(m.exclude_pair, 0)           AS exclude_pair

        FROM {{ ref('int_oda_ar_invoice') }} AS i

        LEFT JOIN {{ ref('int_oda_ar_invoice_payments_agg') }} AS pp
            ON  i.invoice_id = pp.invoice_id
            AND pp.voucher_posted = 1

        LEFT JOIN {{ ref('int_oda_ar_invoice_adjustments_agg') }} AS pa
            ON  i.invoice_id = pa.invoice_id
            AND pa.voucher_posted = 1

        LEFT JOIN {{ ref('int_oda_ar_invoice_netting_agg') }} AS n
            ON  i.invoice_id = n.invoice_id

        LEFT JOIN excluded_invoices AS m
            ON  i.invoice_id = m.invoice_id

        WHERE i.voucher_posted = 1

 )

    select * from ar_remaining_balance