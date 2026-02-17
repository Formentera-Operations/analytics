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
        SELECT
      ariad.invoice_id
    , v.posted                                        AS voucher_posted
    , COALESCE(SUM(ariad.adjustment_detail_amount), 0.00)   AS total_adjustments
        
         FROM {{ref('stg_oda__arinvoiceadjustmentdetail')}} ariad
        
        INNER JOIN {{ref('stg_oda__arinvoiceadjustment') }} aria
        ON aria.id = ariad.invoice_adjustment_id

        INNER JOIN {{ref('stg_oda__voucher_v2')}} v
        ON v.id = aria.voucher_id

        GROUP BY
      ariad.invoice_id
    , v.posted

    )
        select * from ar_adjustment_agg
