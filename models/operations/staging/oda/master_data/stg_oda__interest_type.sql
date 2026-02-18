{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Interest Types

    Source: ODA_BATCH_ODA_INTERESTTYPE (5 rows, batch)
    Grain: One row per interest type (id)

    Notes:
    - Small lookup table — working interest, royalty interest, etc.
    - No CREATEDATE/UPDATEDATE on this table
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_BATCH_ODA_INTERESTTYPE') }}
),

renamed as (
    select
        -- identifiers
        ID::varchar as id,
        INTERESTTYPEIDENTITY::int as interest_type_identity,

        -- attributes
        trim(CODE)::varchar as code,
        trim(NAME)::varchar as name,
        trim(FULLNAME)::varchar as full_name,
        trim(CDEXCODE)::varchar as cdex_code,

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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as interest_type_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        interest_type_sk,

        -- identifiers
        id,
        interest_type_identity,

        -- attributes
        code,
        name,
        full_name,
        cdex_code,

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
