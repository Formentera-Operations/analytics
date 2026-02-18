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
        c.code as company_code,
        c.name as company_name,
        i.owner_id as owner_id,
        e.code as owner_code,
        e.name as owner_name,
        i.well_id as well_id,
        w.code as well_code,
        w.name as well_name,
        i.code as invoice_number,
        i.id as invoice_id,
        i.invoice_type_id as invoice_type_id,
        w.is_hold_all_billing as hold_billing,
        aria.voucher_id as voucher_id,
        aria.adjustment_date as invoice_date,
        ariad.adjustment_detail_amount as total_invoice_amount,
        2 as sort_order,
        case
            when aria.adjustment_type_id = 0 then 'Application of Advance'
            when aria.adjustment_type_id = 1 then replace(ariad.description, 'XClear with Inv#', 'Cross Clear Inv #')
            else 'Adjustment'
        end as invoice_description,
        case
            when aria.adjustment_type_id = 0 then 'AAdv'
            when aria.adjustment_type_id = 1 then 'Xclear'
            else 'Adj.'
        end as invoice_type


    from {{ ref('stg_oda__arinvoiceadjustmentdetail') }} ariad

    inner join {{ ref('stg_oda__arinvoiceadjustment') }} aria
        on ariad.invoice_adjustment_id = aria.id

    inner join {{ ref('stg_oda__voucher_v2') }} v
        on aria.voucher_id = v.id

    inner join {{ ref('stg_oda__arinvoice_v2') }} i
        on ariad.invoice_id = i.id

    inner join {{ ref('stg_oda__company_v2') }} c
        on i.company_id = c.id

    inner join {{ ref('stg_oda__owner_v2') }} o
        on i.owner_id = o.id

    inner join {{ ref('stg_oda__entity_v2') }} e
        on o.entity_id = e.Id

    left join {{ ref('stg_oda__wells') }} w
        on i.well_id = w.id

    where
        i.is_posted
        and v.is_posted

)

select * from ar_adjustments
