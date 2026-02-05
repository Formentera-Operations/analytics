{{ config(
    enabled=true,
    materialized='view'
) }}

{#
    Dimension: Company AR Invoice Summary
    Core Invoice Data (JIB, Advances, Closeout, Misc Invoices)
    
    -- Layer 1: Base Invoice Model
    -- Purpose: Extract and standardize AR invoice data
    -- Dependencies: Base tables only
    
    Sources:
    - stg_oda__arinvoice_v2
    - stg_oda__company_v2
    - stg_oda__owner_v2
    - stg_oda__entity_v2
    - stg_oda__wells
#}

    with ar_invoices as (
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
        i.voucher_id                    as Voucher_ID,
        Case 
            When i.invoice_type_id = 5 Then i.description
            When i.invoice_type_id = 0 Then i.description
            When i.invoice_type_id = 1 Then i.description
            Else w.name
            End As Invoice_Description,
        Case 
            When i.invoice_type_id = 5 Then 'Misc'
            When i.invoice_type_id = 0 Then 'Adv'
            When i.invoice_type_id = 1 Then 'Cls'
            Else 'JIB'
            End As Invoice_Type,
        i.invoice_date                  as Invoice_Date,
        i.invoice_amount                as Total_Invoice_Amount,
        Case
            When i.invoice_type_id = 5 Then 1
            When i.invoice_type_id = 0 Then 2
            Else 1
            End As Sort_Order        
        
        
        
        FROM {{ref('stg_oda__arinvoice_v2') }} i

        INNER JOIN {{ref('stg_oda__company_v2')}} c
        ON c.id = i.company_id
        
        INNER JOIN {{ref('stg_oda__owner_v2')}} o
        ON o.id = i.owner_id

        INNER JOIN {{ref('stg_oda__entity_v2')}} e
        ON e.Id = o.entity_id

       -- LEFT JOIN {{ref('stg_oda__voucher_v2')}} v
       -- ON v.id = i.voucher_id

        LEFT JOIN {{ref('stg_oda__wells')}} w
        ON w.id = i.well_id
        
        Where i.Posted = 1
    )
        select * from ar_invoices

