{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Revenue Deck Sets

    Source: ODA_REVENUEDECKSET (Estuary batch, 19K rows)
    Grain: One row per revenue deck set (id)

    Notes:
    - Groups revisions by product/interest type per well/company
    - Hierarchy: revenue_deck_set → revenue_deck_v2 → revision → participant
    - Integer booleans converted to true/false via coalesce(COL = 1, false)
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_REVENUEDECKSET') }}
),

renamed as (
    select -- noqa: ST06
        -- identifiers
        ID::varchar as id,
        REVENUEDECKSETIDENTITY::int as revenue_deck_set_identity,
        trim(CODE)::varchar as code,
        CODESORT::int as code_sort,

        -- related entities
        COMPANYID::varchar as company_id,
        WELLID::varchar as well_id,
        PRODUCTID::varchar as product_id,

        -- flags
        coalesce(ISDEFAULTDECK = 1, false) as is_default_deck,
        coalesce(ISGASENTITLEMENTDECK = 1, false) as is_gas_entitlement_deck,

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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as revenue_deck_set_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        revenue_deck_set_sk,

        -- identifiers
        id,
        revenue_deck_set_identity,
        code,
        code_sort,

        -- related entities
        company_id,
        well_id,
        product_id,

        -- flags
        is_default_deck,
        is_gas_entitlement_deck,

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
