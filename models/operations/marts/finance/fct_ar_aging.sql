{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'facts']
) }}

{#
    Fact: AR Aging
    -- ArInvoice (base invoices)
    -- PaymentDetails (payments applied)
    -- NetDetails (revenue netting)
    -- AdjustmentDetails (AR adjustments)
    
    -- Layer 5: Final AR Aging Report
    -- Purpose: Final presentation layer for AR aging with all calculations
    -- Dependencies: int_ar_all_details, int_ar_invoice_balances, base voucher table
    
    Sources:
    - int_oda_ar_all_details
    - int_oda_ar_invoice_balances
    - stg_oda__voucher_v2
    
#}
  with ar_aging as (
        SELECT  
        ar.Company_Code         AS Company_Code,
        ar.Company_Name         AS Company_Name,
        ar.Owner_Code           AS Owner_Code,
        ar.Owner_Name           AS Owner_Name,
        ar.Invoice_Number       AS Invoice_Number,
        ar.Well_Code            AS Well_Code,
        ar.Well_Name            AS Well_Name,
        ar.Invoice_Date         AS Transaction_Date,
        REPLACE(ar.Invoice_Description, '   ', '') AS Transaction_Reference,
        vou.code                AS Voucher_Number,
        ar.Invoice_Type         AS Transaction_Type,
        ar.Total_Invoice_Amount AS Balance_Due,
        ar.Sort_Order           AS Sort_Order,
        irb.Remaining_Balance   AS Remaining_Balance,
        Case    
            When (irb.Remaining_Balance <> 0 And irb.exclude_pair = 0)
                Then 1
                Else 0
            End                 AS Include_Record
        
        
        FROM {{ ref('int_oda_ar_all_details') }} ar
        
        LEFT JOIN {{ ref('stg_oda__voucher_v2') }} vou
        ON vou.id = ar.voucher_id

        LEFT JOIN {{ ref('int_oda_ar_invoice_balances') }} irb
        ON ar.invoice_id = irb.invoice_id


    )
        select * from ar_aging
