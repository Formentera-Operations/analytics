{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA AFE Budget headers.

    Source: ODA_AFEBUDGET (Estuary batch, 1.9K rows)
    Grain: One row per AFE budget entry (id)

    Notes:
    - Batch table — has _meta/op column (Estuary puts it on all tables) but NOT CDC
    - Links AFEs to wells and companies with fiscal year and budget basis
    - ISVALUE is already boolean in source — no cast needed
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_AFEBUDGET') }}
),

renamed as (
    select -- noqa: ST06
        -- identifiers
        trim(ID)::varchar as id,
        AFEBUDGETIDENTITY::int as afe_budget_identity,
        NID::int as n_id,

        -- relationships
        trim(AFEID)::varchar as afe_id,
        trim(COMPANYID)::varchar as company_id,
        trim(WELLID)::varchar as well_id,
        trim(CURRENCYID)::varchar as currency_id,

        -- budget configuration
        BASISID::int as basis_id,
        FISCALYEAR::int as fiscal_year,

        -- flags
        ISVALUE::boolean as is_value,

        -- audit
        CREATEDATE::timestamp_ntz as created_at,
        trim(CREATEEVENTID)::varchar as create_event_id,
        UPDATEDATE::timestamp_ntz as updated_at,
        trim(UPDATEEVENTID)::varchar as update_event_id,
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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as afebudget_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        afebudget_sk,

        -- identifiers
        id,
        afe_budget_identity,
        n_id,

        -- relationships
        afe_id,
        company_id,
        well_id,
        currency_id,

        -- budget configuration
        basis_id,
        fiscal_year,

        -- flags
        is_value,

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
