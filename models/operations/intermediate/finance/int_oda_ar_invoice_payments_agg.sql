{{ config(
    enabled=true,
    materialized='view'
) }}

{#
    Dimension: Total payments per invoice with posted/unposted split

    -- Layer 2: Invoice Payments Aggregation
    -- Purpose: Calculate total payments per invoice. Produces one row per invoice_id
    --          (not per posted-flag combination) using conditional SUM to split
    --          posted vs. unposted payment totals.
    --
    -- IMPORTANT: GROUP BY is intentionally on invoice_id only.
    -- Adding posting flags to GROUP BY would create multiple rows per invoice
    -- and fan-out the balance model JOIN — producing incorrect totals.

    Sources:
    - int_oda_ar_payments
#}

with ar_payments_agg as (
    select
        invoice_id,
        sum(total_invoice_amount) as total_payments,
        sum(case when is_invoice_posted then total_invoice_amount else 0 end) as posted_payments,
        sum(case when not is_invoice_posted then total_invoice_amount else 0 end) as unposted_payments

    from {{ ref('int_oda_ar_payments') }}

    group by
        invoice_id
)

select * from ar_payments_agg
