{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Revenue Deck Headers

    Source: ODA_BATCH_ODA_REVENUEDECK_V2 (Estuary batch, 20K rows)
    Grain: One row per revenue deck (id)

    Notes:
    - Revenue deck headers group entity/product/effective date combinations
    - Hierarchy: revenue_deck_set → revenue_deck_v2 → revenue_deck_revision → participant
    - No audit columns (CREATEDATE/UPDATEDATE) in source
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_BATCH_ODA_REVENUEDECK_V2') }}
),

renamed as (
    select
        -- identifiers
        ID::varchar as id,
        REVENUEDECKIDENTITY::int as revenue_deck_identity,
        DECKSETID::varchar as deck_set_id,
        DECKTYPEID::int as deck_type_id,

        -- dates
        EFFECTIVEDATE::date as effective_date,

        -- estuary metadata
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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as revenue_deck_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        revenue_deck_sk,

        -- identifiers
        id,
        revenue_deck_identity,
        deck_set_id,
        deck_type_id,

        -- dates
        effective_date,

        -- estuary metadata
        record_inserted_at,
        record_updated_at,

        -- dbt metadata
        _loaded_at,

        -- ingestion metadata
        _flow_published_at

    from enhanced
)

select * from final
