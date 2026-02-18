{{ config(
    enabled=true,
    materialized='view'
) }}

{#
    Dimension: Company AR Invoice Netting Summary
    Netting Transactions (Revenue Offsets)

    -- Layer 1: Netting Details Model
    -- Purpose: Extract and standardize netting transaction data. Unfiltered —
    --          exposes is_invoice_posted and is_voucher_posted flags so downstream
    --          agg models can split posted vs. unposted netting totals.
    -- Dependencies: Base tables only

    IMPORTANT: netted_amount in ODA is stored as a POSITIVE value.
    The negation (-nd.netted_amount) is applied here to make it additive
    in the balance formula: remaining = invoice + payments + adjustments + net
    DO NOT change this sign convention — it has been E2E validated.

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
        nd.voucher_id as voucher_id,
        'Net' as invoice_type,
        nd.netting_date as invoice_date,
        2 as sort_order,
        i.is_posted as is_invoice_posted,
        v.is_posted as is_voucher_posted,
        coalesce(w.is_hold_all_billing, false) as hold_billing,
        -- Posting status flags — used by netting_agg for posted/unposted splits
        concat(
            'Netted Against Revenue ',
            month(v.voucher_date),
            '/',
            year(v.voucher_date)
        ) as invoice_description,
        -- netted_amount is positive in ODA — negated here so balance is additive
        -nd.netted_amount as total_invoice_amount

    from {{ ref('stg_oda__arinvoicenetteddetail') }} nd

    inner join {{ ref('stg_oda__arinvoice_v2') }} i
        on nd.invoice_id = i.id

    inner join {{ ref('stg_oda__company_v2') }} c
        on i.company_id = c.id

    inner join {{ ref('stg_oda__owner_v2') }} o
        on i.owner_id = o.id

    inner join {{ ref('stg_oda__entity_v2') }} e
        on o.entity_id = e.id

    inner join {{ ref('stg_oda__voucher_v2') }} v
        on nd.voucher_id = v.id

    left join {{ ref('stg_oda__wells') }} w
        on i.well_id = w.id

-- NOTE: No WHERE posted filter — all netting transactions exposed.
-- Use is_invoice_posted / is_voucher_posted flags for filtering.
)

select * from ar_netting
