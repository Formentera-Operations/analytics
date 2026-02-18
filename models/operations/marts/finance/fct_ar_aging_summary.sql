{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'facts']
) }}

{#
    Fact: AR Aging Summary — Invoice-Level with Aging Buckets

    Grain: One row per invoice.

    Aging bucket definition:
    - current_balance:      invoice_date >= today (days_past_due <= 0)
    - balance_1_30_days:    1-30 days past due
    - balance_31_60_days:   31-60 days past due
    - balance_61_90_days:   61-90 days past due
    - balance_90_plus_days: > 90 days past due

    "Current" includes future-dated invoices (negative days_past_due).
    Pre-JIB unposted invoices with future billing dates land in current_balance.
    This is intentional — they represent upcoming billing.

    Use cases:
    - Standard AR aging report: filter WHERE include_record = 1
    - Posted-only aging: add filter WHERE is_invoice_posted = true
    - Pre-JIB preview: filter WHERE is_invoice_posted = false
    - Treasury aging: sum bucket columns filtered to include_record = 1

    Sources:
    - int_oda_ar_invoice
    - int_oda_ar_invoice_remaining_balances
    - stg_oda__voucher_v2
#}

with invoice_base as (
    select
        invoice_id,
        invoice_number,
        company_code,
        company_name,
        owner_id,
        owner_code,
        owner_name,
        well_id,
        well_code,
        well_name,
        invoice_date,
        total_invoice_amount,
        invoice_type_id,
        invoice_type,
        invoice_description,
        voucher_id,
        is_invoice_posted,
        is_voucher_posted,
        hold_billing
    from {{ ref('int_oda_ar_invoice') }}
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
        i.invoice_id,
        i.invoice_number,
        i.company_code,
        i.company_name,
        i.owner_id,
        i.owner_code,
        i.owner_name,
        i.well_id,
        i.well_code,
        i.well_name,
        i.invoice_date,
        i.total_invoice_amount,
        i.invoice_type_id,
        i.invoice_type,
        i.voucher_id,
        v.voucher_number,
        i.is_invoice_posted,
        i.is_voucher_posted,
        i.hold_billing,
        rb.remaining_balance,
        rb.remaining_balance_posted,
        rb.remaining_balance_unposted,
        rb.exclude_pair,
        rb.include_record,
        replace(i.invoice_description, '   ', '') as invoice_description,

        -- Days past due: negative = future-dated (not yet due)
        datediff(day, i.invoice_date, current_date()) as days_past_due,

        -- Standard AR aging buckets based on remaining_balance
        -- BETWEEN is inclusive on both ends; ranges are contiguous with no gaps.
        case
            when datediff(day, i.invoice_date, current_date()) <= 0
                then rb.remaining_balance
            else 0
        end as current_balance,

        case
            when datediff(day, i.invoice_date, current_date()) between 1 and 30
                then rb.remaining_balance
            else 0
        end as balance_1_30_days,

        case
            when datediff(day, i.invoice_date, current_date()) between 31 and 60
                then rb.remaining_balance
            else 0
        end as balance_31_60_days,

        case
            when datediff(day, i.invoice_date, current_date()) between 61 and 90
                then rb.remaining_balance
            else 0
        end as balance_61_90_days,

        -- > 90: no upper bound — catches all long-overdue invoices
        case
            when datediff(day, i.invoice_date, current_date()) > 90
                then rb.remaining_balance
            else 0
        end as balance_90_plus_days

    from invoice_base i
    -- LEFT JOIN (not INNER) — prevents silent row loss if remaining_balances
    -- ever changes and an invoice has no matching record. The not_null test
    -- on remaining_balance in schema.yml makes any mismatch a build error.
    left join remaining_balances rb
        on i.invoice_id = rb.invoice_id
    left join vouchers v
        on i.voucher_id = v.voucher_id
)

select * from final
