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
        SELECT
          invoice_id
        , COALESCE(SUM(netted_amount), 0.00)    AS total_netted
        
        FROM {{ref('stg_oda__arinvoicenetteddetail')}} 

        GROUP BY
        invoice_id
    )
        select * from ar_netting_agg
