{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Accounts Receivable invoice line items.

    Source: ODA_ARINVOICEDETAIL (Estuary CDC, 611 rows)
    Grain: One row per AR invoice detail line (id)

    Notes:
    - Soft deletes filtered via _operation_type != 'd'
    - INVOICEID is the FK to arinvoice_v2 (id)
    - No boolean columns in this table
#}

with

source as (
    select * from {{ source('oda', 'ODA_ARINVOICEDETAIL') }}
),

renamed as (  -- noqa: ST06
    select
        -- identifiers
        ID::varchar as id,
        ARINVOICEDETAILIDENTITY::int as arinvoicedetail_identity,
        INVOICEID::varchar as invoice_id,
        ACCOUNTID::varchar as account_id,
        DISTRIBUTIONCOMPANYID::varchar as distribution_company_id,
        CREATEEVENTID::varchar as create_event_id,
        UPDATEEVENTID::varchar as update_event_id,

        -- descriptive
        trim(DESCRIPTION)::varchar as description,
        ORDINAL::int as ordinal,

        -- financial
        coalesce(DISTRIBUTIONAMOUNT, 0)::decimal(18, 2) as distribution_amount,

        -- volume
        NETVOLUME::decimal(19, 4) as net_volume,

        -- audit
        CREATEDATE::timestamp_ntz as created_at,
        UPDATEDATE::timestamp_ntz as updated_at,
        RECORDINSERTDATE::timestamp_ntz as record_inserted_at,
        RECORDUPDATEDATE::timestamp_ntz as record_updated_at,

        -- ingestion metadata
        "_meta/op"::varchar as _operation_type,
        FLOW_PUBLISHED_AT::timestamp_ntz as _flow_published_at

        -- FLOW_DOCUMENT excluded

    from source
),

filtered as (
    select *
    from renamed
    where
        _operation_type != 'd'
        and id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['id']) }} as arinvoicedetail_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        arinvoicedetail_sk,

        -- identifiers
        id,
        arinvoicedetail_identity,
        invoice_id,
        account_id,
        distribution_company_id,
        create_event_id,
        update_event_id,

        -- descriptive
        description,
        ordinal,

        -- financial
        distribution_amount,

        -- volume
        net_volume,

        -- audit
        created_at,
        updated_at,
        record_inserted_at,
        record_updated_at,

        -- ingestion metadata
        _operation_type,
        _flow_published_at,
        _loaded_at

    from enhanced
)

select * from final
