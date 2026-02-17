{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA User-Defined Fields

    Source: ODA_USERFIELD (Estuary batch, 4.8M rows)
    Grain: One row per custom field value (id)

    Notes:
    - Generic EAV (Entity-Attribute-Value) table for custom attributes
    - Entities can be wells, companies, owners, etc. (determined by entity_type_id)
    - Downstream consumers pivot on user_field_name to get specific values
    - BREAKING CHANGE: Columns renamed from quoted camelCase to snake_case.
      Downstream consumers (int_general_ledger_enhanced, int_gl_enhanced,
      int_oda_wells, dim_wells) must be updated.
    - No audit columns (CREATEDATE/UPDATEDATE) in source
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_USERFIELD') }}
),

renamed as (
    select
        -- identifiers
        ID::varchar as id,
        USERFIELDIDENTITY::int as user_field_identity,
        ENTITYTYPEID::int as entity_type_id,

        -- entity reference
        trim(ENTITYCODE)::varchar as entity_code,
        trim(ENTITYNAME)::varchar as entity_name,

        -- field data
        trim(USERFIELDNAME)::varchar as user_field_name,
        trim(USERFIELDVALUESTRING)::varchar as user_field_value_string,

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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as userfield_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        userfield_sk,

        -- identifiers
        id,
        user_field_identity,
        entity_type_id,

        -- entity reference
        entity_code,
        entity_name,

        -- field data
        user_field_name,
        user_field_value_string,

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
