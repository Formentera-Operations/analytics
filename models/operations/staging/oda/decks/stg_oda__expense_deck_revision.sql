{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Expense Deck Revisions

    Source: ODA_BATCH_ODA_EXPENSEDECKREVISIONVIEW (Estuary batch, 22K rows)
    Grain: One row per expense deck revision (id)

    Notes:
    - Point-in-time snapshots of expense interest allocations
    - Hierarchy: expense_deck_set → expense_deck_v2 → expense_deck_revision → participant
    - Sources from Quorum convenience view (EXPENSEDECKREVISIONVIEW), not base table.
      Sprint 0 flagged this as a re-point candidate, but keeping VIEW source for now
      since both expose the same columns and the VIEW is what was originally configured.
    - Source has "_meta/row_id" and "_meta/op" columns — dropped (batch table, not CDC)
    - Integer booleans converted to true/false via coalesce(COL = 1, false)
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_BATCH_ODA_EXPENSEDECKREVISIONVIEW') }}
),

renamed as (
    select -- noqa: ST06
        -- identifiers
        ID::varchar as id,
        DECKID::varchar as deck_id,
        REVISIONSTATEID::varchar as revision_state_id,

        -- attributes
        trim(NAME)::varchar as name,
        REVISIONNUMBER::int as revision_number,
        trim(CHANGENOTE)::varchar as change_note,

        -- interest calculations
        try_to_double(TOTALINTERESTACTUAL) as total_interest_actual,
        try_to_double(TOTALINTERESTEXPECTED) as total_interest_expected,

        -- flags
        coalesce(ISAFTERCASING = 1, false) as is_after_casing,
        coalesce(USEJIBRATEASOFPAYMENT = 1, false) as is_use_jib_rate_as_of_payment,

        -- status and close
        CLOSEDATE::date as close_date,
        CLOSEBYUSERID::varchar as close_by_user_id,

        -- audit
        CREATEDATE::timestamp_ntz as created_at,
        CREATEEVENTID::varchar as create_event_id,
        UPDATEDATE::timestamp_ntz as updated_at,
        UPDATEEVENTID::varchar as update_event_id,

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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as expense_deck_revision_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        expense_deck_revision_sk,

        -- identifiers
        id,
        deck_id,
        revision_state_id,

        -- attributes
        name,
        revision_number,
        change_note,

        -- interest calculations
        total_interest_actual,
        total_interest_expected,

        -- flags
        is_after_casing,
        is_use_jib_rate_as_of_payment,

        -- status and close
        close_date,
        close_by_user_id,

        -- audit
        created_at,
        create_event_id,
        updated_at,
        update_event_id,

        -- dbt metadata
        _loaded_at,

        -- ingestion metadata
        _flow_published_at

    from enhanced
)

select * from final
