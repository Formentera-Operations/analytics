{{ config(
    enabled=true,
    materialized='view'
) }}

{#
    Dimension: Total netting per invoice with posted/unposted split

    -- Layer 2: Invoice Netting Aggregation
    -- Purpose: Calculate total netting offsets per invoice. Produces one row per
    --          invoice_id using conditional SUM to split posted vs. unposted netting.
    --
    -- IMPORTANT: GROUP BY is intentionally on invoice_id only.
    -- Adding posting flags to GROUP BY would create multiple rows per invoice
    -- and fan-out the balance model JOIN — producing incorrect totals.
    --
    -- NOTE: total_invoice_amount in int_oda_ar_netting is already NEGATED
    -- (-nd.netted_amount). SUM here produces a negative total that correctly
    -- reduces the invoice balance. Do NOT negate again.

    Sources:
    - int_oda_ar_netting
#}

with ar_netting_agg as (
    select
        invoice_id,
        sum(total_invoice_amount) as total_net,
        sum(case when is_invoice_posted then total_invoice_amount else 0 end) as posted_net,
        sum(case when not is_invoice_posted then total_invoice_amount else 0 end) as unposted_net

    from {{ ref('int_oda_ar_netting') }}

    group by
        invoice_id
)

select * from ar_netting_agg
