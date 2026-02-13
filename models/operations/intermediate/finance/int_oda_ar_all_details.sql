{{ config(
    enabled=true,
    materialized='view'
) }}

{#
    Dimension: Unions all AR transaction types into a single dataset
    -- ArInvoice (base invoices)
    -- PaymentDetails (payments applied)
    -- NetDetails (revenue netting)
    -- AdjustmentDetails (AR adjustments)
    
    -- Layer 4: Combined AR Details
    -- Purpose: Union all AR transaction types into a single model
    -- Dependencies: int_ar_invoices, int_ar_payments, int_ar_netting, int_ar_adjustments

    
    Sources:
    - int_oda_ar_invoices
    - int_oda_ar_payments
    - int_ar_netting
    - int_ar_adjustments
#}
  with ar_all_details as (
        SELECT  *
        FROM {{ ref('int_oda_ar_invoice') }}

        UNION All

        SELECT  *
        FROM {{ ref('int_oda_ar_payments') }}

        UNION All
        
        SELECT * 
        FROM {{ ref('int_oda_ar_netting') }}

        UNION All

        SELECT * 
        FROM {{ ref('int_oda_ar_adjustments')}}


    )
        select * from ar_all_details
