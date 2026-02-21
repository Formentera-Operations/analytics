{{
    config(
        materialized='table',
        tags=['marts', 'finance', 'ownership'],
        cluster_by=['well_id', 'owner_id', 'deck_type']
    )
}}

{#
    Mart: Deck Interest History (Revenue + Expense)

    Purpose: Unified history of ownership interest allocations across both revenue
    and expense decks. One row per deck participant, with a deck_type discriminator
    distinguishing revenue vs expense entries.

    Grain: One row per deck participant (participant_id)
    Row count: ~197M (196M revenue + 877K expense)

    Join chain (4-hop):
    participant → revision → deck_v2 → deck_set (well_id, effective_date, product_id)

    Revenue-specific columns: product_id, suspend_category_id, is_auto_suspend_payment
    Expense deck_set has NO product_id column → NULL for expense rows.

    Enrichment joins:
    - dim_owners → owner_name, owner_code
    - dim_wells → well_code, eid, well_name
    - stg_oda__interest_type → interest_type_code, interest_type_name

    Investigation findings (2026-02-20):
    - Revenue: 99.9% resolve through full chain to well_id
    - Expense: 100% resolve through full chain to well_id
    - Expense participants: 97.4% have owner_id, 2.6% have company_id (no overlap)
    - Interest types: T=Total, W=Working, R=Royalty, O=Override, S=Same as redistributed

    Downstream consumers:
    - bridge_well_owner (current M:N snapshot)
    - Sprint 2: dim_company_WI/NRI refactor
#}

with

-- =============================================================================
-- Revenue deck participants with full join chain
-- =============================================================================
revenue_participants as (
    select
        p.id as participant_id,
        'revenue' as deck_type,

        -- entity details (from participant)
        p.owner_id,
        p.company_id,
        p.entity_type_id,
        p.interest_type_id,
        p.decimal_interest,
        p.custom_interest_type_id,
        p.is_memo,

        -- revenue-specific
        p.suspend_category_id,
        p.is_auto_suspend_payment,

        -- revision details
        p.deck_revision_id,
        rev.revision_number,
        rev.close_date as revision_close_date,

        -- deck header
        dk.effective_date,

        -- deck set (well + product)
        ds.well_id,
        ds.product_id,
        ds.code as deck_code,

        -- audit
        p.created_at,
        p.updated_at,
        p._loaded_at,
        p._flow_published_at

    from {{ ref('stg_oda__revenue_deck_participant') }} p
    left join {{ ref('stg_oda__revenue_deck_revision') }} rev
        on p.deck_revision_id = rev.id
    left join {{ ref('stg_oda__revenue_deck_v2') }} dk
        on rev.deck_id = dk.id
    left join {{ ref('stg_oda__revenue_deck_set') }} ds
        on dk.deck_set_id = ds.id
),

-- =============================================================================
-- Expense deck participants with full join chain
-- =============================================================================
expense_participants as (
    select
        p.id as participant_id,
        'expense' as deck_type,

        -- entity details (from participant)
        p.owner_id,
        p.company_id,
        p.entity_type_id,
        p.interest_type_id,
        p.decimal_interest,
        p.custom_interest_type_id,
        p.is_memo,

        -- revenue-specific (NULL for expense)
        null::varchar as suspend_category_id,
        false as is_auto_suspend_payment,

        -- revision details
        p.deck_revision_id,
        rev.revision_number,
        rev.close_date as revision_close_date,

        -- deck header
        dk.effective_date,

        -- deck set (well only — expense has no product_id)
        ds.well_id,
        null::varchar as product_id,
        ds.code as deck_code,

        -- audit
        p.created_at,
        p.updated_at,
        p._loaded_at,
        p._flow_published_at

    from {{ ref('stg_oda__expense_deck_participant') }} p
    left join {{ ref('stg_oda__expense_deck_revision') }} rev
        on p.deck_revision_id = rev.id
    left join {{ ref('stg_oda__expense_deck_v2') }} dk
        on rev.deck_id = dk.id
    left join {{ ref('stg_oda__expense_deck_set') }} ds
        on dk.deck_set_id = ds.id
),

-- =============================================================================
-- Union revenue + expense
-- =============================================================================
unified as (
    select * from revenue_participants
    union all
    select * from expense_participants
),

-- =============================================================================
-- Enrichment lookups
-- =============================================================================
owners as (
    select
        owner_id,
        owner_code,
        owner_name
    from {{ ref('dim_owners') }}
),

wells as (
    select
        well_id,
        well_code,
        well_name
    from {{ ref('dim_wells') }}
),

well_eids as (
    select
        oda_well_id,
        eid
    from {{ ref('well_360') }}
    where oda_well_id is not null
),

interest_types as (
    select
        interest_type_identity as interest_type_id,
        code as interest_type_code,
        name as interest_type_name
    from {{ ref('stg_oda__interest_type') }}
),

-- =============================================================================
-- Final enriched output
-- =============================================================================
final as (
    select
        -- =================================================================
        -- Surrogate Key
        -- =================================================================
        {{ dbt_utils.generate_surrogate_key(['u.participant_id', 'u.deck_type']) }}
            as deck_interest_history_sk,

        -- =================================================================
        -- Identifiers
        -- =================================================================
        u.participant_id,
        u.deck_type,
        u.deck_revision_id,

        -- =================================================================
        -- Well
        -- =================================================================
        u.well_id,
        w.well_code,
        w.well_name,
        we.eid,

        -- =================================================================
        -- Owner / Entity
        -- =================================================================
        u.owner_id,
        o.owner_code,
        o.owner_name,
        u.company_id,
        u.entity_type_id,

        -- =================================================================
        -- Interest Details
        -- =================================================================
        u.interest_type_id,
        it.interest_type_code,
        it.interest_type_name,
        u.decimal_interest,
        u.custom_interest_type_id,
        u.is_memo,

        -- =================================================================
        -- Deck Context
        -- =================================================================
        u.effective_date,
        u.deck_code,
        u.revision_number,
        u.revision_close_date,
        u.product_id,

        -- =================================================================
        -- Revenue-Specific
        -- =================================================================
        u.suspend_category_id,
        u.is_auto_suspend_payment,

        -- =================================================================
        -- Audit
        -- =================================================================
        u.created_at,
        u.updated_at,
        u._loaded_at,
        u._flow_published_at

    from unified u
    left join owners o on u.owner_id = o.owner_id
    left join wells w on u.well_id = w.well_id
    left join well_eids we on u.well_id = we.oda_well_id
    left join interest_types it
        on u.interest_type_id = it.interest_type_id
)

select * from final
