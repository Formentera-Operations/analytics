{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Revenue Deck Participants

    Source: ODA_REVENUEDECKPARTICIPANT (Estuary batch, 196M rows)
    Grain: One row per revenue deck participant (id)

    Notes:
    - 2nd largest ODA table after GL — MUST remain materialized as view
    - Owner-level revenue interest allocations per revision
    - Hierarchy: revenue_deck_set → revenue_deck_v2 → revision → revenue_deck_participant
    - Integer booleans converted to true/false via coalesce(COL = 1, false)
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_REVENUEDECKPARTICIPANT') }}
),

renamed as (
    select -- noqa: ST06
        -- identifiers
        ID::varchar as id,
        REVENUEDECKPARTICIPANTIDENTITY::int as revenue_deck_participant_identity,
        DECKREVISIONID::varchar as deck_revision_id,

        -- entity details
        ENTITYTYPEID::int as entity_type_id,
        COMPANYID::varchar as company_id,
        OWNERID::varchar as owner_id,

        -- interest and suspense
        INTERESTTYPEID::int as interest_type_id,
        try_to_double(DECIMALINTEREST) as decimal_interest,
        CUSTOMINTERESTTYPEID::int as custom_interest_type_id,
        SUSPENSECATEGORYID::varchar as suspend_category_id,

        -- flags
        coalesce(ISMEMO = 1, false) as is_memo,
        coalesce(AUTOSUSPENDPAYMENT = 1, false) as is_auto_suspend_payment,

        -- audit
        CREATEDATE::timestamp_ntz as created_at,
        CREATEEVENTID::varchar as create_event_id,
        UPDATEDATE::timestamp_ntz as updated_at,
        UPDATEEVENTID::varchar as update_event_id,
        RECORDINSERTDATE::timestamp_ntz as record_inserted_at,
        RECORDUPDATEDATE::timestamp_ntz as record_updated_at,

        -- ingestion metadata
        FLOW_PUBLISHED_AT::timestamp_tz as _flow_published_at

    from source
),

filtered as (
    select *
    from renamed
    where id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['id']) }} as revenue_deck_participant_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        revenue_deck_participant_sk,

        -- identifiers
        id,
        revenue_deck_participant_identity,
        deck_revision_id,

        -- entity details
        entity_type_id,
        company_id,
        owner_id,

        -- interest and suspense
        interest_type_id,
        decimal_interest,
        custom_interest_type_id,
        suspend_category_id,

        -- flags
        is_memo,
        is_auto_suspend_payment,

        -- audit
        created_at,
        create_event_id,
        updated_at,
        update_event_id,
        record_inserted_at,
        record_updated_at,

        -- dbt metadata
        _loaded_at,

        -- ingestion metadata
        _flow_published_at

    from enhanced
)

select * from final
