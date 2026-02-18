{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Purchasers (V2)

    Source: ODA_BATCH_ODA_PURCHASER_V2 (183 rows, batch)
    Grain: One row per purchaser (id)

    Notes:
    - Purchaser entities linked to ODA entities via entity_id
    - Integer booleans converted to true/false via coalesce(COL = 1, false)
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_BATCH_ODA_PURCHASER_V2') }}
),

renamed as (
    select -- noqa: ST06
        -- identifiers
        ID::varchar as id,
        PURCHASERV2IDENTITY::int as purchaser_v2_identity,
        ENTITYID::varchar as entity_id,

        -- attributes
        trim(CDEXCODE)::varchar as cdex_code,

        -- flags
        coalesce(ACTIVE = 1, false) as is_active,
        coalesce(BYWELLREVENUERECEIVABLE = 1, false) as is_by_well_revenue_receivable,

        -- audit
        CREATEDATE::timestamp_ntz as created_at,
        CREATEEVENTID::varchar as create_event_id,
        UPDATEDATE::timestamp_ntz as updated_at,
        UPDATEEVENTID::varchar as update_event_id,

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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as purchaser_v2_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        purchaser_v2_sk,

        -- identifiers
        id,
        purchaser_v2_identity,
        entity_id,

        -- attributes
        cdex_code,

        -- flags
        is_active,
        is_by_well_revenue_receivable,

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
