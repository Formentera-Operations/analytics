{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Account Types

    Source: ODA_ACCOUNTTYPE (2 rows, batch)
    Grain: One row per account type (id)

    Notes:
    - No CREATEDATE/UPDATEDATE in source — only Estuary metadata timestamps
    - Column names prefixed with type_ to distinguish from other type lookups
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_ACCOUNTTYPE') }}
),

renamed as (
    select
        -- identifiers
        ID::varchar as id,

        -- descriptors
        trim(CODE)::varchar as type_code,
        trim(NAME)::varchar as type_name,
        trim(FULLNAME)::varchar as type_full_name,

        -- audit
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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as account_types_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        account_types_sk,

        -- identifiers
        id,

        -- descriptors
        type_code,
        type_name,
        type_full_name,

        -- audit
        record_inserted_at,
        record_updated_at,

        -- dbt metadata
        _loaded_at,

        -- ingestion metadata
        _flow_published_at

    from enhanced
)

select * from final
