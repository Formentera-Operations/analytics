{{ config(
    enabled=true,
    materialized='view'
) }}

{#
    Dimension: Invoice remaining balances with advance/closeout pair exclusion applied.

    -- Layer 4: Remaining Balance + Exclusion Logic
    -- Purpose: Combines balance arithmetic from int_oda_ar_invoice_balances with
    --          advance/closeout pair exclusion from int_oda_ar_advance_closeout_pairs.
    --          Separated from int_oda_ar_invoice_balances for single-responsibility.
    --
    -- Materialization: view (not ephemeral) — two materialized table facts consume
    --          this model. As a view it can be inspected via `dbt show` during
    --          validation of the financially-sensitive exclusion logic.
    --
    -- Exclusion logic:
    --   Advance and closeout invoice pairs are flagged exclude_pair = 1 and
    --   excluded from aging reports (include_record = 0). These are fully
    --   netted pairs that represent no real AR outstanding balance.
    --
    -- NOTE: Uses UNION (not UNION ALL) for excluded_invoice_ids to deduplicate
    --   in case an invoice appears in multiple pair relationships. NULL guards
    --   prevent unmatched LEFT JOIN rows from entering the exclusion set.
    --
    -- NOTE: stg_oda__aradvancecloseout currently has 0 rows. When data arrives,
    --   verify pair logic with unfiltered invoice base (Open Question 2 from brainstorm).

    Sources:
    - int_oda_ar_invoice_balances
    - int_oda_ar_advance_closeout_pairs
#}

with balances as (
    select
        invoice_id,
        is_invoice_posted,
        invoice_amount,
        total_payments,
        total_adjustments,
        total_net,
        remaining_balance,
        remaining_balance_posted,
        remaining_balance_unposted
    from {{ ref('int_oda_ar_invoice_balances') }}
),

-- UNION (not UNION ALL) deduplicates invoice_ids that appear in multiple pairs.
-- NULL guards exclude unmatched LEFT JOIN rows from int_oda_ar_advance_closeout_pairs.
excluded_invoice_ids as (
    select advance_invoice_id as invoice_id
    from {{ ref('int_oda_ar_advance_closeout_pairs') }}
    where advance_invoice_id is not null

    union distinct

    select closeout_invoice_id as invoice_id
    from {{ ref('int_oda_ar_advance_closeout_pairs') }}
    where closeout_invoice_id is not null
),

final as (
    select
        b.invoice_id,
        b.is_invoice_posted,
        b.invoice_amount,
        b.total_payments,
        b.total_adjustments,
        b.total_net,
        b.remaining_balance,
        b.remaining_balance_posted,
        b.remaining_balance_unposted,
        case when e.invoice_id is not null then 1 else 0 end as exclude_pair,
        case
            when
                b.remaining_balance != 0
                and e.invoice_id is null
                then 1
            else 0
        end as include_record
    from balances b
    left join excluded_invoice_ids e
        on b.invoice_id = e.invoice_id
)

select * from final
