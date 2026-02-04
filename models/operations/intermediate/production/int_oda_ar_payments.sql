{{ config(
    enabled=true,
    materialized='view'
) }}

{#
    Dimension: Company AR Invoice Payment Summary
    Payment Transactions applied to invoices
    
    -- Layer 1: Payment Details Model
    -- Purpose: Extract and standardize payment transaction data
    -- Dependencies: Base tables only
    
    Sources:
    - stg_oda__arinvoicepayment
    - stg_oda__arinvoicepaymentdetail
    - stg_oda__arinvoice_v2
    - stg_oda__voucher_v2
    - stg_oda__company_v2
    - stg_oda__owner_v2
    - stg_oda__entity_v2
    - stg_oda__wells
#}

  with ar_payments as (
        select 
        c.code                          as Company_Code,
        c.name                          as Company_Name,
        i.owner_id                      as Owner_ID,
        e.code                          as Owner_Code,
        e.name                          as Owner_Name,
        i.well_id                       as Well_ID,
        w.code                          as Well_Code,
        w.name                          as Well_Name,
        i.code                          as Invoice_Number,
        i.id                            as Invoice_ID,
        i.invoice_type_id               as Invoice_Type_ID,
        w.hold_all_billing              as Hold_Billing,
        p.voucher_id                    as Voucher_ID,
        CONCAT('Payment, Check # ', p.payee_check_number) as Invoice_Description,
        'Pymt'                          as Invoice_Type,
        p.payment_date                  as Invoice_Date,
        pd.amount_applied               as Total_Invoice_Amount,
        2                               as Sort_Order
        
        
        FROM {{ref('stg_oda__arinvoicepaymentdetail')}} pd
        
        INNER JOIN {{ref('stg_oda__arinvoicepayment')}} p
        ON p.id = pd.invoice_payment_id

        INNER JOIN {{ref('stg_oda__voucher_v2')}} v
        ON v.id = p.voucher_id

        INNER JOIN {{ref('stg_oda__arinvoice_v2') }} i
        ON i.id = pd.invoice_id

        INNER JOIN {{ref('stg_oda__company_v2')}} c
        ON c.id = i.company_id
        
        INNER JOIN {{ref('stg_oda__owner_v2')}} o
        ON o.id = i.owner_id

        INNER JOIN {{ref('stg_oda__entity_v2')}} e
        ON e.Id = o.entity_id

        LEFT JOIN {{ref('stg_oda__wells')}} w
        ON w.id = i.well_id
        
        Where i.Posted = 1
        AND v.posted = 1
    )
        select * from ar_payments