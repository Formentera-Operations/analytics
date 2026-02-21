{{
    config(
        materialized='table',
        tags=['marts', 'finance', 'ownership']
    )
}}

{#
    Bridge: Well × Owner (M:N)

    Purpose: Current ownership interest snapshot for each well-owner-deck_type
    combination. Sources from fct_deck_interest_history and filters to the latest
    revision per well+owner+deck_type.

    Grain: One row per well × owner × deck_type (M:N bridge)

    Selection logic:
    - Latest effective_date per (well_id, owner_id, deck_type, interest_type_id)
    - Tie-break by revision_number DESC (most recent revision wins)
    - Excludes memo-only participants (is_memo = true)

    Use cases:
    - "Who has interest in this well?" lookups
    - Ownership interest validation
    - WI/NRI aggregation per well
#}

with

latest_interest as (
    select
        participant_id,
        deck_type,
        well_id,
        well_code,
        well_name,
        eid,
        owner_id,
        owner_code,
        owner_name,
        interest_type_id,
        interest_type_code,
        interest_type_name,
        decimal_interest,
        effective_date,
        revision_number
    from {{ ref('fct_deck_interest_history') }}
    where
        well_id is not null
        and owner_id is not null
        and not is_memo
    qualify row_number() over (
        partition by well_id, owner_id, deck_type, interest_type_id
        order by effective_date desc, revision_number desc
    ) = 1
),

final as (
    select
        -- =================================================================
        -- Surrogate Key
        -- =================================================================
        {{ dbt_utils.generate_surrogate_key([
            'well_id', 'owner_id', 'deck_type', 'interest_type_id'
        ]) }} as well_owner_sk,

        -- =================================================================
        -- Well
        -- =================================================================
        well_id,
        well_code,
        well_name,
        eid,

        -- =================================================================
        -- Owner
        -- =================================================================
        owner_id,
        owner_code,
        owner_name,

        -- =================================================================
        -- Interest
        -- =================================================================
        deck_type,
        interest_type_id,
        interest_type_code,
        interest_type_name,
        decimal_interest,

        -- =================================================================
        -- Context
        -- =================================================================
        effective_date,
        revision_number,

        -- =================================================================
        -- Metadata
        -- =================================================================
        current_timestamp() as _refreshed_at

    from latest_interest
)

select * from final
