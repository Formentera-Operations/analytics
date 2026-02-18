{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Entity master data

    Source: ODA_BATCH_ODA_ENTITY_V2 (56K rows, batch)
    Grain: One row per entity (id)

    Notes:
    - Core dimension — entities are the parent of vendors and owners
    - CODE is TEXT in source (despite numeric-looking values) — cast to varchar
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_BATCH_ODA_ENTITY_V2') }}
),

renamed as (
    select
        -- identifiers
        ID::varchar as id,
        trim(CODE)::varchar as code,
        CODESORT::varchar as code_sort,
        trim(NAME)::varchar as name,
        trim(FULLNAME)::varchar as full_name,
        ENTITY_V2IDENTITY::int as entity_v2_identity,

        -- tax information
        TAXID::varchar as tax_id,
        TAXIDTYPEID::varchar as tax_id_type_id,
        trim(NAME1099)::varchar as name_1099,

        -- contact information
        MAINCONTACTID::varchar as main_contact_id,

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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as entity_v2_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        entity_v2_sk,

        -- identifiers
        id,
        code,
        code_sort,
        name,
        full_name,
        entity_v2_identity,

        -- tax information
        tax_id,
        tax_id_type_id,
        name_1099,

        -- contact information
        main_contact_id,

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
