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
        SELECT
        i.company_code
        , i.company_name
        , i.owner_id
        , i.owner_code
        , i.owner_name
        , i.well_id
        , i.well_code
        , i.well_name
        , i.invoice_number
        , i.invoice_id
        , i.invoice_type_id
        , i.hold_billing
        , aria.voucher_id
        , CASE
            WHEN aria.adjustment_type_id = 0
                THEN 'Application of Advance'
            WHEN aria.adjustment_type_id = 1
                THEN REPLACE(ariad.description, 'XClear with Inv#', 'Cross Clear Inv #')
            ELSE 'Adjustment'
        END                                 AS invoice_description
        , CASE
            WHEN aria.adjustment_type_id = 0 THEN 'AAdv'
            WHEN aria.adjustment_type_id = 1 THEN 'Xclear'
            ELSE 'Adj.'
        END                                 AS invoice_type
        , aria.adjustment_date                AS invoice_date
        , ariad.adjustment_detail_amount      AS total_invoice_amount
        , 2                                   AS sort_order

        FROM {{ ref('stg_oda__arinvoiceadjustmentdetail') }} AS ariad

        INNER JOIN {{ ref('stg_oda__arinvoiceadjustment') }} AS aria
            ON aria.id = ariad.invoice_adjustment_id

        INNER JOIN {{ ref('stg_oda__voucher_v2') }} AS v
            ON v.id = aria.voucher_id

        INNER JOIN {{ ref('int_oda_ar_invoice') }} AS i
            ON i.invoice_id = ariad.invoice_id

        WHERE i.voucher_posted = 1
        AND v.posted         = 1
        
    )
        select * from ar_adjustments