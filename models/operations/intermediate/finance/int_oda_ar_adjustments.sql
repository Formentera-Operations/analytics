{{ config(
    enabled=true,
    materialized='view'
) }}

{#
    Dimension: Company AR Invoice Adjustments
    Adjustment transactions (advances applied, cross-clears, etc.)
    
    -- Layer 1: Adjustment Details Model
    -- Purpose: Extract and standardize adjustment transaction data
    -- Dependencies: Base tables only
    
    Sources:
    - stg_oda__arinvoiceadjustment
    - stg_oda__arinvoiceadjustmentdetail
    - stg_oda__arinvoice_v2
    - stg_oda__voucher_v2
    - stg_oda__company_v2
    - stg_oda__owner_v2
    - stg_oda__entity_v2
    - stg_oda__wells
#}
with ar_adjustments as (
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
        aria.voucher_id                 as Voucher_ID,
        Case 
            When aria.adjustment_type_id = 0 Then 'Application of Advance'
            When aria.adjustment_type_id = 1 Then REPLACE(ariad.Description, 'XClear with Inv#', 'Cross Clear Inv#')
            Else 'Adjustment'
            End As Invoice_Description,
        Case 
            When aria.adjustment_type_id = 0 Then 'AAdv'
            When aria.adjustment_type_id = 1 Then 'Xclear'
            Else 'Adj'
            End As Invoice_Type,
        aria.adjustment_date                  as Invoice_Date,
        ariad.adjustment_detail_amount        as Total_Invoice_Amount,
        2                                     as Sort_Order
        
        
        FROM {{ref('stg_oda__arinvoiceadjustmentdetail')}} ariad
        
        INNER JOIN {{ref('stg_oda__arinvoiceadjustment') }} aria
        ON aria.id = ariad.invoice_adjustment_id

        INNER JOIN {{ref('stg_oda__voucher_v2')}} v
        ON v.id = aria.voucher_id

        INNER JOIN {{ref('stg_oda__arinvoice_v2')}} i
        ON i.id = ariad.invoice_id

        INNER JOIN {{ref('stg_oda__company_v2')}} c
        ON c.id = i.company_id
        
        INNER JOIN {{ref('stg_oda__owner_v2')}} o
        ON o.id = i.owner_id

        INNER JOIN {{ref('stg_oda__entity_v2')}} e
        ON e.Id = o.entity_id

        LEFT JOIN {{ref('stg_oda__wells')}} w
        ON w.id = i.well_id
        
        Where i.Posted = 1
        and v.Posted = 1
        
    )
        select * from ar_adjustments