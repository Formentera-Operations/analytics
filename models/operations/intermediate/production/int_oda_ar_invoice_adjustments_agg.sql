{{ config(
    enabled=true,
    materialized='view'
) }}

{#
    Dimension: Total adjustments per invoice
    
    -- Layer 2: Invoice Adjustments Aggregation
    -- Purpose: Calculate total adjustments per invoice
    -- Dependencies: int_ar_adjustments
    
    Sources:
    - int_oda_ar_adjustments
#}
  with ar_adjustment_agg as (
        select 
        invoice_id,
        SUM(Total_Invoice_Amount) as Total_Net,
        
        FROM {{ ref('int_oda_ar_adjustments') }}

        GROUP BY
        invoice_id
    )
        select * from ar_adjustment_agg
