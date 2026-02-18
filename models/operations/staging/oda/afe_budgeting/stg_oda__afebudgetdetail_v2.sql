{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA AFE Budget Detail lines.

    Source: ODA_AFEBUDGETDETAIL_V2 (Estuary batch, 424K rows)
    Grain: One row per budget detail line item (id)

    Notes:
    - Batch table — has _meta/op column (Estuary puts it on all tables) but NOT CDC
    - Monthly budget amounts for AFE budget entries
    - MONTH is a Snowflake reserved word — quoted as "MONTH" in source CTE
    - Renamed to budget_month to avoid downstream reserved word issues
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_AFEBUDGETDETAIL_V2') }}
),

renamed as (
    select
        -- identifiers
        trim(ID)::varchar as id,
        AFEBUDGETDETAIL_V2IDENTITY::int as afe_budget_detail_v2_identity,
        trim(AFEBUDGETENTRYID)::varchar as afe_budget_entry_id,

        -- financial
        AMOUNT::decimal(18, 2) as amount,
        "MONTH"::int as budget_month, -- noqa: RF06

        -- audit
        CREATEDATE::timestamp_ntz as created_at,
        trim(CREATEEVENTID)::varchar as create_event_id,
        UPDATEDATE::timestamp_ntz as updated_at,
        trim(UPDATEEVENTID)::varchar as update_event_id,
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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as afebudgetdetail_v2_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        afebudgetdetail_v2_sk,

        -- identifiers
        id,
        afe_budget_detail_v2_identity,
        afe_budget_entry_id,

        -- financial
        amount,
        budget_month,

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
