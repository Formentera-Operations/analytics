{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Expense Deck Headers

    Source: ODA_BATCH_ODA_EXPENSEDECK_V2 (Estuary batch, 9.7K rows)
    Grain: One row per expense deck (id)

    Notes:
    - Expense deck headers group entity/product/effective date combinations
    - Hierarchy: expense_deck_set → expense_deck_v2 → expense_deck_revision → participant
    - Source has both UPDATEDATE and RECORDUPDATEDATE (different timestamps)
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_BATCH_ODA_EXPENSEDECK_V2') }}
),

renamed as (
    select
        -- identifiers
        ID::varchar as id,
        EXPENSEDECKIDENTITY::int as expense_deck_identity,
        DECKSETID::varchar as deck_set_id,
        NID::int as n_id,

        -- dates
        EFFECTIVEDATE::date as effective_date,

        -- audit
        UPDATEDATE::timestamp_ntz as updated_at,
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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as expense_deck_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        expense_deck_sk,

        -- identifiers
        id,
        expense_deck_identity,
        deck_set_id,
        n_id,

        -- dates
        effective_date,

        -- audit
        updated_at,
        record_inserted_at,
        record_updated_at,

        -- dbt metadata
        _loaded_at,

        -- ingestion metadata
        _flow_published_at

    from enhanced
)

select * from final
