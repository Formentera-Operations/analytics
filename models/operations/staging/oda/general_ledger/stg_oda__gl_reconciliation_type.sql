{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA GL Reconciliation Types

    Source: ODA_GLRECONCILIATIONTYPE (8 rows, batch)
    Grain: One row per reconciliation type (id)

    Notes:
    - No CREATEDATE/UPDATEDATE in source — only Estuary metadata timestamps
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_GLRECONCILIATIONTYPE') }}
),

renamed as (
    select
        -- identifiers
        ID::varchar as id,
        GLRECONCILIATIONTYPEIDENTITY::int as gl_reconciliation_type_identity,

        -- descriptors
        trim(CODE)::varchar as code,
        trim(NAME)::varchar as name,
        trim(FULLNAME)::varchar as full_name,

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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as gl_reconciliation_type_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        gl_reconciliation_type_sk,

        -- identifiers
        id,
        gl_reconciliation_type_identity,

        -- descriptors
        code,
        name,
        full_name,

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
