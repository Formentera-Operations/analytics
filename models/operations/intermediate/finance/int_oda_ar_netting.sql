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
    , nd.voucher_id
    , CONCAT('Netted Against Revenue '
           , MONTH(vn.voucher_date), '/'
           , YEAR(vn.voucher_date))       AS invoice_description
    , 'Net'                               AS invoice_type
    , nd.netting_date                     AS invoice_date
    , -nd.netted_amount                   AS total_invoice_amount
    , 2                                   AS sort_order

        FROM {{ ref('stg_oda__arinvoicenetteddetail') }} AS nd

        INNER JOIN {{ ref('stg_oda__voucher_v2') }} AS vn
            ON vn.id = nd.voucher_id

        INNER JOIN {{ ref('int_oda_ar_invoice') }} AS i
            ON i.invoice_id = nd.invoice_id

        WHERE i.voucher_posted = 1
        
    )
        select * from ar_netting