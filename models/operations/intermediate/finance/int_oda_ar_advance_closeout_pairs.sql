{{ config(
    enabled=true,
    materialized='view'
) }}

{#
    Dimension: Identifies advance/closeout pairs that should be excluding from aging 
    
    -- Layer 3: Advance Closeout Pairs
    -- Purpose: Identify advance/closeout invoice pairs for exclusion logic
    -- Dependencies: int_ar_invoices, base tables
    
    Sources:
    - int_oda_ar_invoice
    - stg_oda__aradvancecloseout
#}

with ar_advance_closeout as (
        select 
        cls.invoice_id          AS closeout_invoice_id,
        adv.invoice_id          AS advance_invoice_id,
        1                       AS exclude_pair
        
        FROM {{ref('stg_oda__aradvancecloseout')}} ac

        INNER JOIN {{ref('int_oda_ar_invoice')}} cls
        ON ac.voucher_id = cls.voucher_id
        AND cls.invoice_type_id = 1

        LEFT JOIN {{ref('int_oda_ar_invoice')}} adv
        ON ac.target_voucher_id = adv.voucher_id
        AND adv.invoice_type_id = 0
      
    )
        select * from ar_advance_closeout 
