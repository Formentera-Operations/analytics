{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA AR advance closeout records.

    Source: ODA_BATCH_ODA_ARADVANCECLOSEOUT (Estuary batch, 0 rows — schema only)
    Grain: One row per advance closeout (id)

    Notes:
    - Table currently has 0 rows; schema exists for future data
    - VOUCHERID = original advance voucher being closed out
    - TARGETVOUCHERID = target/settlement voucher
    - Join direction in int_oda_ar_advance_closeout_pairs must be verified when rows arrive
    - Batch table (not CDC) — no soft delete filtering
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_BATCH_ODA_ARADVANCECLOSEOUT') }}
),

renamed as (
    select -- noqa: ST06
        -- identifiers
        trim(id)::varchar as id,
        aradvancecloseoutidentity::int as ar_advance_closeout_identity,

        -- relationships
        trim(voucherid)::varchar as voucher_id,
        trim(targetvoucherid)::varchar as target_voucher_id,
        trim(wellid)::varchar as well_id,

        -- audit
        createdate::timestamp_ntz as created_at,
        trim(createeventid)::varchar as create_event_id,
        updatedate::timestamp_ntz as updated_at,
        trim(updateeventid)::varchar as update_event_id,
        recordinsertdate::timestamp_ntz as record_inserted_at,
        recordupdatedate::timestamp_ntz as record_updated_at,

        -- ingestion metadata
        flow_published_at::timestamp_tz as _flow_published_at

    from source
),

filtered as (
    select *
    from renamed
    where id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['id']) }} as aradvancecloseout_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        aradvancecloseout_sk,

        -- identifiers
        id,
        ar_advance_closeout_identity,

        -- relationships
        voucher_id,
        target_voucher_id,
        well_id,

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
