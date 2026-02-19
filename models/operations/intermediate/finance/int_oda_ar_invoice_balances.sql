{{ config(
    enabled=true,
    materialized='view'
) }}

{#
    Dimension: Calculates remaining balance per invoice considering all transaction types.
    Produces both a combined balance and posted/unposted split components.

    -- Layer 3: Invoice Balance Calculation
    -- Purpose: Balance arithmetic only — one row per invoice_id, no exclusion logic.
    --          Advance/closeout exclusion has been extracted to
    --          int_oda_ar_invoice_remaining_balances for separation of concerns.
    --
    -- Balance formula (E2E validated):
    --   remaining_balance = invoice_amount + payments + adjustments + net
    --   where:
    --     payments      = negative (amount_applied is negative in ODA)
    --     adjustments   = mixed sign (pass through unchanged)
    --     net           = negative (negated from positive ODA netted_amount)

    Sources:
    - int_oda_ar_invoice
    - int_oda_ar_invoice_payments_agg
    - int_oda_ar_invoice_adjustments_agg
    - int_oda_ar_invoice_netting_agg
#}

with ar_invoice_balances as (
    select
        i.invoice_id as invoice_id,
        i.is_invoice_posted as is_invoice_posted,
        i.total_invoice_amount as invoice_amount,

        coalesce(p.total_payments, 0) as total_payments,
        coalesce(a.total_adjustments, 0) as total_adjustments,
        coalesce(n.total_net, 0) as total_net,

        -- Combined remaining balance (posted + unposted)
        i.total_invoice_amount
        + coalesce(p.total_payments, 0)
        + coalesce(a.total_adjustments, 0)
        + coalesce(n.total_net, 0) as remaining_balance,

        -- Posted-only balance: invoice amount (if posted) + posted transaction components
        case when i.is_invoice_posted then i.total_invoice_amount else 0 end
        + coalesce(p.posted_payments, 0)
        + coalesce(a.posted_adjustments, 0)
        + coalesce(n.posted_net, 0) as remaining_balance_posted,

        -- Unposted-only balance: invoice amount (if unposted) + unposted transaction components
        case when not i.is_invoice_posted then i.total_invoice_amount else 0 end
        + coalesce(p.unposted_payments, 0)
        + coalesce(a.unposted_adjustments, 0)
        + coalesce(n.unposted_net, 0) as remaining_balance_unposted

    from {{ ref('int_oda_ar_invoice') }} i

    left join {{ ref('int_oda_ar_invoice_payments_agg') }} p
        on i.invoice_id = p.invoice_id

    left join {{ ref('int_oda_ar_invoice_adjustments_agg') }} a
        on i.invoice_id = a.invoice_id

    left join {{ ref('int_oda_ar_invoice_netting_agg') }} n
        on i.invoice_id = n.invoice_id

-- NOTE: Advance/closeout pair exclusion logic has been moved to
-- int_oda_ar_invoice_remaining_balances to separate balance arithmetic
-- from business exclusion rules.
)

select * from ar_invoice_balances
