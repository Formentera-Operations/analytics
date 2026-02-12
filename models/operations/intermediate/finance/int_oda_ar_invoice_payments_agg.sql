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
        select 
        invoice_id,
        SUM(Total_Invoice_Amount) as Total_Payments,
        
        FROM {{ ref('int_oda_ar_payments') }}

        GROUP BY
        invoice_id
    )
        select * from ar_payments_agg
