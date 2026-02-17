{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Revision States

    Source: ODA_REVISIONSTATE (Estuary batch, 3 rows)
    Grain: One row per revision state (id)

    Notes:
    - Tiny lookup table: draft, approved, superseded
    - Used by revenue/expense deck revision models
    - No audit columns (CREATEDATE/UPDATEDATE) in source
#}

with

source as (
    select * from {{ source('oda', 'ODA_REVISIONSTATE') }}
),

renamed as (
    select
        -- identifiers
        ID::varchar as id,
        REVISIONSTATEIDENTITY::int as revision_state_identity,

        -- attributes
        trim(NAME)::varchar as name,
        trim(FULLNAME)::varchar as full_name,

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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as revision_state_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        revision_state_sk,

        -- identifiers
        id,
        revision_state_identity,

        -- attributes
        name,
        full_name,

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
