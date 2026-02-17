{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Source Modules

    Source: ODA_BATCH_ODA_SOURCEMODULE (Estuary batch, 26 rows)
    Grain: One row per source module (id)

    Notes:
    - Tiny lookup table for ODA module identifiers (GL, AP, AR, JIB, etc.)
    - Referenced by int_general_ledger_enhanced and int_gl_enhanced via code join
    - No audit columns (CREATEDATE/UPDATEDATE) in source
#}

with

source as (
    select * from {{ source('oda', 'ODA_BATCH_ODA_SOURCEMODULE') }}
),

renamed as (
    select
        -- identifiers
        ID::varchar as id,
        SOURCEMODULEIDENTITY::int as source_module_identity,

        -- attributes
        trim(CODE)::varchar as code,
        trim(NAME)::varchar as name,
        trim(FULLNAME)::varchar as full_name,
        trim(MODULE)::varchar as module,

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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as source_module_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        source_module_sk,

        -- identifiers
        id,
        source_module_identity,

        -- attributes
        code,
        name,
        full_name,
        module,

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
