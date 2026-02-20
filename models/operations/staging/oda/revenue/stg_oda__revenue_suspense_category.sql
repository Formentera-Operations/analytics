{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Revenue Suspense Categories.

    Source: ODA_BATCH_ODA_REVENUESUSPENSECATEGORY (Estuary batch, 24 rows)
    Grain: One row per suspense category (id)

    Notes:
    - Batch table — no CDC, no soft-delete filtering
    - Lookup table for reasons revenue payments are held in suspense
      (e.g., title issues, missing tax ID, disputed interest)
    - Referenced by stg_oda__owner_v2.default_revenue_suspense_category_id
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
    - Fixed typo from prior version: revemue → revenue
#}

with

source as (
    select * from {{ source('oda', 'ODA_BATCH_ODA_REVENUESUSPENSECATEGORY') }}
),

renamed as (
    select
        -- identifiers
        trim(ID)::varchar as id,
        REVENUESUSPENSECATEGORYIDENTITY::int as revenue_suspense_category_identity,
        trim(CODE)::varchar as code,
        trim(NAME)::varchar as name,
        trim(FULLNAME)::varchar as full_name,

        -- audit
        CREATEDATE::timestamp_ntz as created_at,
        UPDATEDATE::timestamp_ntz as updated_at,
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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as revenue_suspense_category_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        revenue_suspense_category_sk,

        -- identifiers
        id,
        revenue_suspense_category_identity,
        code,
        name,
        full_name,

        -- audit
        created_at,
        updated_at,
        record_inserted_at,
        record_updated_at,

        -- dbt metadata
        _loaded_at,

        -- ingestion metadata
        _flow_published_at

    from enhanced
)

select * from final
