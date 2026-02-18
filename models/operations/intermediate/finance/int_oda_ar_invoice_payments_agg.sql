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
        sum(Total_Invoice_Amount) as total_payments

    from {{ ref('int_oda_ar_payments') }}

    group by
        invoice_id
)

select * from ar_payments_agg
