{{
    config(
        materialized='view',
        tags=['intermediate', 'production', 'ownership']
    )
}}

{#
    Intermediate: Latest Company Net Revenue Interest (Revenue Deck)

    Purpose: FP Company NRI per revenue deck — latest version, latest effective date,
    deck code = 1 for Company 200 (FP Operations). Product-level interest breakdown.

    Grain: One row per well × company participant × product (NRI by product).

    Filters:
    - Deck set company = Company 200 (FP Operations)
    - Deck code = 1
    - Latest revision (revision state = 1, close_date IS NULL)
    - Latest effective date per deck set (QUALIFY window function)
    - Company participants only (participant.company_id IS NOT NULL)
    - import_data_id IS NULL (excludes Quorum DataHub duplication)

    Dependencies:
    - stg_oda__revenue_deck_participant
    - stg_oda__revenue_deck_revision
    - stg_oda__revenue_deck_v2
    - stg_oda__revenue_deck_set
    - stg_oda__wells
    - stg_oda__company_v2
    - stg_oda__product
    - stg_oda__revision_state

    Downstream consumers:
    - dim_company_NRI (INNER JOIN to stg_cc__company_wells)
    - fct_company_WI_NRI_calc (via dim_company_NRI)
#}

with

-- =============================================================================
-- Deck chain: participant → revision → deck → deck_set → well/product
-- =============================================================================
deck_participants as (
    select
        rdp.company_id as participant_company_id,
        rdp.interest_type_id,
        rdp.decimal_interest,
        rdr.name as deck_name,
        rdr.revision_number,
        rdr.created_at,
        rdr.updated_at,
        rd.effective_date,
        rd.deck_set_id,
        rds.well_id,
        rds.product_id,
        rds.company_id as deck_set_company_id,
        rds.code as deck_code
    from {{ ref('stg_oda__revenue_deck_participant') }} rdp
    inner join {{ ref('stg_oda__revenue_deck_revision') }} rdr
        on rdp.deck_revision_id = rdr.id
    inner join {{ ref('stg_oda__revenue_deck_v2') }} rd
        on rdr.deck_id = rd.id
    inner join {{ ref('stg_oda__revenue_deck_set') }} rds
        on rd.deck_set_id = rds.id
    inner join {{ ref('stg_oda__revision_state') }} rs
        on rdr.revision_state_id = rs.id
    where
        rdr.import_data_id is null
        and rds.company_id = '57d809a6-7302-ee11-bd5d-f40669ee7a09'
        and rs.id = '1'
        and rdr.close_date is null
        and rdp.company_id is not null
        and rds.code = '1'
    qualify rd.effective_date = max(rd.effective_date) over (
        partition by rd.deck_set_id
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

products as (
    select
        id as product_id,
        name as product_name
    from {{ ref('stg_oda__product') }}
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
        'NRI' as interest_type,
        cast(
            sum(dp.decimal_interest) * 100 as decimal(12, 8)
        ) as total_interest,
        dp.created_at,
        dp.updated_at,
        right(w.well_code, 6) as eid,
        concat(co.company_code, ': ', co.company_name) as company_code_name,
        coalesce(p.product_name, 'All Products') as product_name

    from deck_participants dp
    left join wells w on dp.well_id = w.well_id
    left join companies co on dp.participant_company_id = co.company_id
    left join products p on dp.product_id = p.product_id
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
        p.product_name,
        dp.created_at,
        dp.updated_at
)

select * from final
