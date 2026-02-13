{{ config(
    enabled=true,
    materialized='view'
) }}

{#
    Dimension: Total payments per invoice
    
    -- Layer 2: Invoice Netting Aggregation
    -- Purpose: Calculate total netting per invoice
    -- Dependencies: int_ar_netting
    
    Sources:
    - int_oda_ar_netting
#}

    with ar_netting_agg as (
        select 
        invoice_id,
        SUM(Total_Invoice_Amount) as Total_Net,
        
        FROM {{ ref('int_oda_ar_netting') }}

        GROUP BY
        invoice_id
    )
        select * from ar_netting_agg
