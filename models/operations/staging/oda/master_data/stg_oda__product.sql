{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Products

    Source: ODA_BATCH_ODA_PRODUCT (9 rows, batch)
    Grain: One row per product (id)

    Notes:
    - Small lookup table — oil, gas, NGL, water, etc.
    - Price fields and conversion factor cast to float
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_BATCH_ODA_PRODUCT') }}
),

renamed as (
    select
        -- identifiers
        ID::varchar as id,
        PRODUCTIDENTITY::int as product_identity,
        TYPEID::varchar as type_id,
        CURRENCYID::varchar as currency_id,

        -- attributes
        trim(CODE)::varchar as code,
        CODESORT::int as code_sort,
        trim(NAME)::varchar as name,
        trim(FULLNAME)::varchar as full_name,
        trim(CDEXCODE)::varchar as cdex_code,

        -- pricing
        LOWESTTYPICALPRICE::float as lowest_typical_price,
        HIGHESTTYPICALPRICE::float as highest_typical_price,
        CONVERSIONTOEQUIVALENTBARRELS::float as conversion_to_equivalent_barrels,

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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as product_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        product_sk,

        -- identifiers
        id,
        product_identity,
        type_id,
        currency_id,

        -- attributes
        code,
        code_sort,
        name,
        full_name,
        cdex_code,

        -- pricing
        lowest_typical_price,
        highest_typical_price,
        conversion_to_equivalent_barrels,

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
