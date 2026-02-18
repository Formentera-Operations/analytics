{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Revenue Deck Revisions

    Source: ODA_REVENUEDECKREVISION (Estuary batch, 346K rows)
    Grain: One row per revenue deck revision (id)

    Notes:
    - Point-in-time snapshots of revenue interest allocations
    - Hierarchy: revenue_deck_set → revenue_deck_v2 → revenue_deck_revision → participant
    - Contains NRI actual and total interest expected values
    - close_date indicates closed/locked revisions
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_REVENUEDECKREVISION') }}
),

renamed as (
    select -- noqa: ST06
        -- identifiers
        ID::varchar as id,
        REVENUEDECKREVISIONIDENTITY::int as revenue_deck_revision_identity,
        DECKID::varchar as deck_id,
        REVISIONSTATEID::varchar as revision_state_id,
        IMPORTDATAID::varchar as import_data_id,

        -- attributes
        trim(NAME)::varchar as name,
        REVISIONNUMBER::int as revision_number,
        trim(CHANGENOTE)::varchar as change_note,

        -- interest calculations
        try_to_double(TOTALINTERESTEXPECTED) as total_interest_expected,
        try_to_double(NRIACTUAL) as nri_actual,

        -- status and close
        CLOSEDATE::date as close_date,
        CLOSEBYUSERID::varchar as close_by_user_id,

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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as revenue_deck_revision_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        revenue_deck_revision_sk,

        -- identifiers
        id,
        revenue_deck_revision_identity,
        deck_id,
        revision_state_id,
        import_data_id,

        -- attributes
        name,
        revision_number,
        change_note,

        -- interest calculations
        total_interest_expected,
        nri_actual,

        -- status and close
        close_date,
        close_by_user_id,

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
