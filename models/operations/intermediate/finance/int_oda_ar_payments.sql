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
        p.voucher_id as voucher_id,
        'Pymt' as invoice_type,
        p.payment_date as invoice_date,
        pd.amount_applied as total_invoice_amount,
        2 as sort_order,
        concat('Payment, Check # ', p.payee_check_number) as invoice_description


    from {{ ref('stg_oda__arinvoicepaymentdetail') }} pd

    inner join {{ ref('stg_oda__arinvoicepayment') }} p
        on pd.invoice_payment_id = p.id

    inner join {{ ref('stg_oda__voucher_v2') }} v
        on p.voucher_id = v.id

    inner join {{ ref('stg_oda__arinvoice_v2') }} i
        on pd.invoice_id = i.id

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

select * from ar_payments
