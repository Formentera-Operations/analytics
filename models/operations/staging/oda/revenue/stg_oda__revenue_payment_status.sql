{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Revenue Payment Status lookup.

    Source: ODA_BATCH_ODA_REVENUEPAYMENTSTATUS (Estuary batch, small lookup)
    Grain: One row per payment status (id)

    Notes:
    - Batch table — no CDC soft delete filtering needed (_meta/op is always 'c')
    - No deduplication needed — Estuary batch handles dedup at the connector level
    - ID is NUMBER(38,0) — integer primary key, not a GUID. Cast as ::int
    - REVENUEPAYMENTSTATUSIDENTITY also NUMBER(38,0) → ::int
    - No CREATEDATE/UPDATEDATE/event ID audit columns in source
    - _meta/op excluded — batch table, never contains 'd' operations
    - FLOW_DOCUMENT excluded — large JSON blob, not needed downstream
    - Validated against information_schema.columns on 2026-02-20
#}

with

source as (
    select * from {{ source('oda', 'ODA_BATCH_ODA_REVENUEPAYMENTSTATUS') }}
),

renamed as (
    select -- noqa: ST06
        -- identifiers
        ID::int                                     as id,
        REVENUEPAYMENTSTATUSIDENTITY::int           as revenue_payment_status_identity,

        -- descriptive
        trim(CODE)::varchar                         as code,
        trim(NAME)::varchar                         as name,
        trim(FULLNAME)::varchar                     as full_name,

        -- audit
        RECORDINSERTDATE::timestamp_ntz             as record_inserted_at,
        RECORDUPDATEDATE::timestamp_ntz             as record_updated_at,

        -- ingestion metadata
        FLOW_PUBLISHED_AT::timestamp_tz             as _flow_published_at

    from source
),

filtered as (
    select *
    from renamed
    where id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['id']) }} as revenue_payment_status_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        revenue_payment_status_sk,

        -- identifiers
        id,
        revenue_payment_status_identity,

        -- descriptive
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