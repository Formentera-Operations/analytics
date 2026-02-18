{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Expense Deck Participants

    Source: ODA_BATCH_ODA_EXPENSEDECKPARTICIPANT (Estuary batch, 877K rows)
    Grain: One row per expense deck participant (id)

    Notes:
    - Owner-level expense interest allocations per revision
    - Hierarchy: expense_deck_set → expense_deck_v2 → revision → expense_deck_participant
    - Integer booleans converted to true/false via coalesce(COL = 1, false)
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_BATCH_ODA_EXPENSEDECKPARTICIPANT') }}
),

renamed as (
    select -- noqa: ST06
        -- identifiers
        ID::varchar as id,
        EXPENSEDECKPARTICIPANTIDENTITY::int as expense_deck_participant_identity,
        DECKREVISIONID::varchar as deck_revision_id,

        -- entity details
        ENTITYTYPEID::int as entity_type_id,
        COMPANYID::varchar as company_id,
        OWNERID::varchar as owner_id,

        -- interest information
        INTERESTTYPEID::int as interest_type_id,
        try_to_double(DECIMALINTEREST) as decimal_interest,
        CUSTOMINTERESTTYPEID::int as custom_interest_type_id,

        -- flags
        coalesce(ISMEMO = 1, false) as is_memo,

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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as expense_deck_participant_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        expense_deck_participant_sk,

        -- identifiers
        id,
        expense_deck_participant_identity,
        deck_revision_id,

        -- entity details
        entity_type_id,
        company_id,
        owner_id,

        -- interest information
        interest_type_id,
        decimal_interest,
        custom_interest_type_id,

        -- flags
        is_memo,

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
