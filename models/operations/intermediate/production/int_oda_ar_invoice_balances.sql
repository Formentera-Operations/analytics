{{ config(
    enabled=true,
    materialized='view'
) }}

{#
    Dimension: Calculates remaining balance per invoice considering all transaction types
    
    -- Layer 3: Invoice Remaining Balances
    -- Purpose: Calculate remaining balance for each invoice considering all transactions
    -- Dependencies: int_ar_invoices, int_ar_invoice_payments_agg, int_ar_invoice_adjustments_agg, int_ar_invoice_netting_agg, int_ar_advance_closeout_pairs
    
    Sources:
    - int_oda_ar_invoice
    - int_ar_invoice_payments_agg
    - int_ar_invoice_adjustments_agg
    - int_ar_invoice_netting_agg
    - int_ar_advance_closeout_pairs
#}

with ar_invoice_balances as (
        select 

        i.invoice_id                            AS invoice_id,
        i.Total_Invoice_Amount 
         + coalesce(p.Total_Payments, 0) 
        + coalesce(a.Total_Adjustments, 0)
        + coalesce(n.Total_Net, 0)     AS Remaining_Balance,
         coalesce(m.exclude_pair, 0)    AS exclude_pair
        
        FROM {{ ref('int_oda_ar_invoice') }} i 
        
        LEFT JOIN {{ref('int_oda_ar_invoice_payments_agg')}} p
        ON i.invoice_id = p.invoice_id

        LEFT JOIN {{ref('int_oda_ar_invoice_adjustments_agg')}} a
        ON i.invoice_id = a.invoice_id

        LEFT JOIN {{ref('int_oda_ar_invoice_netting_agg')}} n
        ON i.invoice_id = n.invoice_id

        LEFT JOIN 
            (
                select 
                advance_invoice_id  AS  invoice_id,
                1                   AS  exclude_pair
                FROM {{ref('int_oda_ar_advance_closeout_pairs')}} 

                UNION All

                select 
                closeout_invoice_id  AS invoice_id,
                1                   AS exclude_pair
                FROM {{ref('int_oda_ar_advance_closeout_pairs')}} 

            ) m

            ON i.invoice_id = m.invoice_id
        
    )
        select * from ar_invoice_balances