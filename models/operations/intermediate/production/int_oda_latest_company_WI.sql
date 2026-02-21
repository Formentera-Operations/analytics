{{
    config(
        materialized='view',
        tags=['intermediate', 'production', 'ownership']
    )
}}

{#
    Intermediate: Latest Company Working Interest (Expense Deck)

    Purpose: FP Company WI per expense deck — latest version, latest effective date,
    deck code = 1 for Company 200 (FP Operations).

    Grain: One row per well × company participant (total interest summed across
    interest types for that company in the well's expense deck).

    Filters:
    - Deck set company = Company 200 (FP Operations)
    - Deck code = 1
    - Latest revision (revision state = 1, close_date IS NULL)
    - Latest effective date per deck set (QUALIFY window function)
    - Company participants only (participant.company_id IS NOT NULL)

    Dependencies:
    - stg_oda__expense_deck_participant
    - stg_oda__expense_deck_revision
    - stg_oda__expense_deck_v2
    - stg_oda__expense_deck_set
    - stg_oda__wells
    - stg_oda__company_v2
    - stg_oda__revision_state

    Downstream consumers:
    - dim_company_WI (INNER JOIN to stg_cc__company_wells)
    - fct_company_WI_NRI_calc (via dim_company_WI)

    Note: bridge_well_owner tracks OWNER participants (97.4% of expense deck).
    This model tracks COMPANY participants (2.6%). A deeper refactor to source
    from fct_deck_interest_history is planned when deck_set_company_id is added
    to the unified fact.
#}

with

-- =============================================================================
-- Deck chain: participant → revision → deck → deck_set → well
-- =============================================================================
deck_participants as (
    select
        edp.company_id as participant_company_id,
        edp.interest_type_id,
        edp.decimal_interest,
        edr.name as deck_name,
        edr.revision_number,
        edr.created_at,
        edr.updated_at,
        ed.effective_date,
        ed.deck_set_id,
        eds.well_id,
        eds.company_id as deck_set_company_id,
        eds.code as deck_code
    from {{ ref('stg_oda__expense_deck_participant') }} edp
    inner join {{ ref('stg_oda__expense_deck_revision') }} edr
        on edp.deck_revision_id = edr.id
    inner join {{ ref('stg_oda__expense_deck_v2') }} ed
        on edr.deck_id = ed.id
    inner join {{ ref('stg_oda__expense_deck_set') }} eds
        on ed.deck_set_id = eds.id
    inner join {{ ref('stg_oda__revision_state') }} rs
        on edr.revision_state_id = rs.id
    where
        eds.company_id = '57d809a6-7302-ee11-bd5d-f40669ee7a09'
        and rs.id = '1'
        and edr.close_date is null
        and edp.company_id is not null
        and eds.code = '1'
    qualify ed.effective_date = max(ed.effective_date) over (
        partition by ed.deck_set_id
    )
),

-- =============================================================================
-- Dimension lookups
-- =============================================================================
wells as (
    select
        id as well_id,
        code as well_code,
        name as well_name,
        api_number,
        state_code,
        county_name
    from {{ ref('stg_oda__wells') }}
),

companies as (
    select
        id as company_id,
        code as company_code,
        name as company_name
    from {{ ref('stg_oda__company_v2') }}
),

-- =============================================================================
-- Aggregated output
-- =============================================================================
final as (
    select
        w.well_code,
        w.well_name,
        w.api_number,
        w.state_code,
        w.county_name,
        dp.deck_name,
        dp.effective_date as latest_effective_date,
        dp.revision_number,
        'WI' as interest_type,
        cast(
            sum(dp.decimal_interest) * 100 as decimal(12, 8)
        ) as total_interest,
        dp.created_at,
        dp.updated_at,
        right(w.well_code, 6) as eid,
        concat(co.company_code, ': ', co.company_name) as company_code_name

    from deck_participants dp
    left join wells w on dp.well_id = w.well_id
    left join companies co on dp.participant_company_id = co.company_id
    group by
        w.well_code,
        w.well_name,
        w.api_number,
        w.state_code,
        w.county_name,
        dp.deck_name,
        dp.effective_date,
        dp.revision_number,
        co.company_code,
        co.company_name,
        dp.created_at,
        dp.updated_at
)

select * from final
