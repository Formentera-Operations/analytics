{{ config(
    enabled=true,
    materialized='view'
) }}

{#
    Dimension: Total adjustments per invoice with posted/unposted split

    -- Layer 2: Invoice Adjustments Aggregation
    -- Purpose: Calculate total adjustments per invoice. Produces one row per invoice_id
    --          (not per posted-flag combination) using conditional SUM to split
    --          posted vs. unposted adjustment totals.
    --
    -- IMPORTANT: GROUP BY is intentionally on invoice_id only.
    -- Adding posting flags to GROUP BY would create multiple rows per invoice
    -- and fan-out the balance model JOIN — producing incorrect totals.
    --
    -- NOTE: adjustment_detail_amount is mixed sign in ODA (negative for advance
    -- applications, positive/negative for cross-clears). Pass through unchanged.

    Sources:
    - int_oda_ar_adjustments
#}

with ar_adjustment_agg as (
    select
        invoice_id,
        sum(total_invoice_amount) as total_adjustments,
        sum(case when is_invoice_posted then total_invoice_amount else 0 end) as posted_adjustments,
        sum(case when not is_invoice_posted then total_invoice_amount else 0 end) as unposted_adjustments

    from {{ ref('int_oda_ar_adjustments') }}

    group by
        invoice_id
)

select * from ar_adjustment_agg
