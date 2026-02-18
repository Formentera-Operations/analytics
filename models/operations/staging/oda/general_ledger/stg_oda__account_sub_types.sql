{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Account Sub Types

    Source: ODA_BATCH_ODA_ACCOUNTSUBTYPE (5 rows, batch)
    Grain: One row per account subtype (id)

    Notes:
    - No CREATEDATE/UPDATEDATE in source — only Estuary metadata timestamps
    - Integer boolean converted to true/false via coalesce(COL = 1, false)
    - Column names prefixed with subtype_ to distinguish from other type lookups
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_BATCH_ODA_ACCOUNTSUBTYPE') }}
),

renamed as (
    select -- noqa: ST06
        -- identifiers
        ID::varchar as id,
        ACCOUNTTYPEID::varchar as account_type_id,

        -- descriptors
        trim(CODE)::varchar as subtype_code,
        trim(NAME)::varchar as subtype_name,
        trim(FULLNAME)::varchar as subtype_full_name,

        -- flags
        coalesce(NORMALLYDEBIT = 1, false) as is_normally_debit,

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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as account_sub_types_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        account_sub_types_sk,

        -- identifiers
        id,
        account_type_id,

        -- descriptors
        subtype_code,
        subtype_name,
        subtype_full_name,

        -- flags
        is_normally_debit,

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
