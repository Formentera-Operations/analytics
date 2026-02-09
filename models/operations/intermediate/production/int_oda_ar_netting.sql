{{ config(
    enabled=true,
    materialized='view'
) }}

{#
    Dimension: Company AR Invoice Netting Summary
    Netting Transactions (Revenue Offsets)
    
    -- Layer 1: Netting Details Model
    -- Purpose: Extract and standardize netting transaction data
    -- Dependencies: Base tables only
    
    Sources:
    - stg_oda__arinvoicenetteddetail
    - stg_oda__arinvoice_v2
    - stg_oda__voucher_v2
    - stg_oda__company_v2
    - stg_oda__owner_v2
    - stg_oda__entity_v2
    - stg_oda__wells
#}

with ar_netting as (
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
        nd.voucher_id                   as Voucher_ID,
        CONCAT('Netted Against Revenue ', MONTH(v.voucher_date), '/', YEAR(v.voucher_date)) as Invoice_Description,
        'Net'                           as Invoice_Type,
        nd.netting_date                 as Invoice_Date,
        -nd.netted_amount               as Total_Invoice_Amount,
        2                               as Sort_Order
        
        
        FROM {{ref('stg_oda__arinvoicenetteddetail')}} nd
        
        INNER JOIN {{ref('stg_oda__arinvoice_v2') }} i
        ON i.id = nd.invoice_id

        INNER JOIN {{ref('stg_oda__company_v2')}} c
        ON c.id = i.company_id
        
        INNER JOIN {{ref('stg_oda__owner_v2')}} o
        ON o.id = i.owner_id

        INNER JOIN {{ref('stg_oda__entity_v2')}} e
        ON e.Id = o.entity_id

        INNER JOIN {{ref('stg_oda__voucher_v2')}} v
        ON v.id = nd.voucher_id

        LEFT JOIN {{ref('stg_oda__wells')}} w
        ON w.id = i.well_id
        
        Where i.Posted = 1
        
    )
        select * from ar_netting