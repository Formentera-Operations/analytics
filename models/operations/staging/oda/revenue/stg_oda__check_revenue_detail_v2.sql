{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Check Revenue Detail V2.

    Source: ODA_CHECKREVENUEDETAIL_V2 (Estuary batch, ~46.2M rows)
    Grain: One row per check-to-detail allocation (id)

    Notes:
    - Batch table — no CDC soft delete filtering needed (_meta/op is always 'c')
    - No deduplication needed — Estuary batch handles dedup at the connector level
    - VOIDED is native BOOLEAN type in information_schema
    - Timestamps are TIMESTAMP_LTZ in source → cast to ::timestamp_ntz per convention
    - _meta/op excluded — batch table, never contains 'd' operations
    - FLOW_DOCUMENT excluded — large JSON blob, not needed downstream
    - Validated against information_schema.columns on 2026-02-20
#}

with

source as (
    select * from {{ source('oda', 'ODA_CHECKREVENUEDETAIL_V2') }}
),

renamed as (
    select
        -- identifiers
        ID::varchar as id,
        CHECKREVENUEDETAIL_V2IDENTITY::int as check_revenue_detail_v2_identity,
        CHECKREVENUEID::varchar as check_revenue_id,
        OWNERREVENUEDETAILID::varchar as owner_revenue_detail_id,
        WELLID::varchar as well_id,

        -- working / non-working splits
        NETVALUEWORKING::float as net_value_working,
        NETVALUENONWORKING::float as net_value_non_working,
        NETVOLUMEWORKING::float as net_volume_working,
        NETVOLUMENONWORKING::float as net_volume_non_working,

        -- payment
        PAYMENTAMOUNT::float as payment_amount,

        -- flags
        VOIDED::boolean as is_voided,

        -- references
        TRIALOWNERREVENUEDETAILID::varchar as trial_owner_revenue_detail_id,

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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as check_revenue_detail_v2_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        check_revenue_detail_v2_sk,

        -- identifiers
        id,
        check_revenue_detail_v2_identity,
        check_revenue_id,
        owner_revenue_detail_id,
        well_id,

        -- working / non-working splits
        net_value_working,
        net_value_non_working,
        net_volume_working,
        net_volume_non_working,

        -- payment
        payment_amount,

        -- flags
        is_voided,

        -- references
        trial_owner_revenue_detail_id,

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
