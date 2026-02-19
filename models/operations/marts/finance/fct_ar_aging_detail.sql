{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'facts']
) }}

{#
    Fact: AR Aging Detail — Transaction-Level

    Grain: One row per AR transaction (invoice, payment, adjustment, or netting).
    Every transaction row carries the parent invoice's remaining_balance and
    include_record flag so consumers can filter to actionable outstanding items
    without needing a separate join.

    Use cases:
    - Drill-down from invoice summary to individual transactions
    - Pre-JIB AR preview: filter WHERE is_invoice_posted = false
    - Posted AR aging: filter WHERE is_invoice_posted = true AND include_record = 1

    Sources:
    - int_oda_ar_invoice
    - int_oda_ar_payments
    - int_oda_ar_adjustments
    - int_oda_ar_netting
    - int_oda_ar_invoice_remaining_balances
    - stg_oda__voucher_v2
#}

with invoices as (
    select
        invoice_id,
        company_code,
        company_name,
        owner_id,
        owner_code,
        owner_name,
        well_id,
        well_code,
        well_name,
        invoice_number,
        invoice_type_id,
        voucher_id,
        invoice_date as transaction_date,
        total_invoice_amount as transaction_amount,
        invoice_description as transaction_reference,
        invoice_type as transaction_type,
        is_invoice_posted,
        is_voucher_posted,
        hold_billing,
        sort_order,
        'invoice' as transaction_source
    from {{ ref('int_oda_ar_invoice') }}
),

payments as (
    select
        invoice_id,
        company_code,
        company_name,
        owner_id,
        owner_code,
        owner_name,
        well_id,
        well_code,
        well_name,
        invoice_number,
        invoice_type_id,
        voucher_id,
        invoice_date as transaction_date,
        total_invoice_amount as transaction_amount,
        invoice_description as transaction_reference,
        invoice_type as transaction_type,
        is_invoice_posted,
        is_voucher_posted,
        hold_billing,
        sort_order,
        'payment' as transaction_source
    from {{ ref('int_oda_ar_payments') }}
),

adjustments as (
    select
        invoice_id,
        company_code,
        company_name,
        owner_id,
        owner_code,
        owner_name,
        well_id,
        well_code,
        well_name,
        invoice_number,
        invoice_type_id,
        voucher_id,
        invoice_date as transaction_date,
        total_invoice_amount as transaction_amount,
        invoice_description as transaction_reference,
        invoice_type as transaction_type,
        is_invoice_posted,
        is_voucher_posted,
        hold_billing,
        sort_order,
        'adjustment' as transaction_source
    from {{ ref('int_oda_ar_adjustments') }}
),

netting as (
    select
        invoice_id,
        company_code,
        company_name,
        owner_id,
        owner_code,
        owner_name,
        well_id,
        well_code,
        well_name,
        invoice_number,
        invoice_type_id,
        voucher_id,
        invoice_date as transaction_date,
        total_invoice_amount as transaction_amount,
        invoice_description as transaction_reference,
        invoice_type as transaction_type,
        is_invoice_posted,
        is_voucher_posted,
        hold_billing,
        sort_order,
        'netting' as transaction_source
    from {{ ref('int_oda_ar_netting') }}
),

-- Explicit column list required in UNION ALL — column order differs across models.
-- SELECT * in UNION ALL resolves by position, not name, causing type mismatches.
all_transactions as (
    select * from invoices
    union all
    select * from payments
    union all
    select * from adjustments
    union all
    select * from netting
),

remaining_balances as (
    select
        invoice_id,
        remaining_balance,
        remaining_balance_posted,
        remaining_balance_unposted,
        exclude_pair,
        include_record
    from {{ ref('int_oda_ar_invoice_remaining_balances') }}
),

vouchers as (
    select
        id as voucher_id,
        code as voucher_number
    from {{ ref('stg_oda__voucher_v2') }}
),

final as (
    select
        t.invoice_id,
        t.transaction_source,
        t.company_code,
        t.company_name,
        t.owner_id,
        t.owner_code,
        t.owner_name,
        t.well_id,
        t.well_code,
        t.well_name,
        t.invoice_number,
        t.invoice_type_id,
        t.voucher_id,
        v.voucher_number,
        t.transaction_date,
        t.transaction_type,
        t.transaction_amount as balance_due,
        rb.remaining_balance,
        rb.remaining_balance_posted,
        rb.remaining_balance_unposted,
        rb.exclude_pair,
        rb.include_record,
        t.is_invoice_posted,
        t.is_voucher_posted,
        t.hold_billing,
        t.sort_order,
        replace(t.transaction_reference, '   ', '') as transaction_reference
    from all_transactions t
    left join remaining_balances rb
        on t.invoice_id = rb.invoice_id
    left join vouchers v
        on t.voucher_id = v.voucher_id
)

select * from final
