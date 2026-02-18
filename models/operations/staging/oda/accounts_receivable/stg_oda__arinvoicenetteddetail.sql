{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA AR invoice netted detail records.

    Source: ODA_ARINVOICENETTEDDETAIL (Estuary batch, 531K rows)
    Grain: One row per netted revenue detail (id)

    Notes:
    - Batch table — no CDC soft delete filtering needed
    - Links invoices to wells and owner revenue details with netting amounts
    - Volume columns use decimal(19,4) for BOE/MCF precision
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_ARINVOICENETTEDDETAIL') }}
),

renamed as (
    select -- noqa: ST06
        -- identifiers
        ID::varchar as id,
        ARINVOICENETTEDDETAILIDENTITY::int as arinvoicenetteddetail_identity,
        trim(INVOICEID)::varchar as invoice_id,
        trim(OWNERREVENUEDETAILID)::varchar as owner_revenue_detail_id,
        trim(VOUCHERID)::varchar as voucher_id,
        trim(WELLID)::varchar as well_id,

        -- dates
        ACCRUALDATE::date as accrual_date,
        NETTINGDATE::date as netting_date,

        -- financial
        coalesce(NETVALUENONWORKING, 0)::decimal(18, 2) as net_value_non_working,
        coalesce(NETVALUEWORKING, 0)::decimal(18, 2) as net_value_working,
        coalesce(NETTEDAMOUNT, 0)::decimal(18, 2) as netted_amount,

        -- volumes
        coalesce(NETVOLUMENONWORKING, 0)::decimal(19, 4) as net_volume_non_working,
        coalesce(NETVOLUMEWORKING, 0)::decimal(19, 4) as net_volume_working,

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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as arinvoicenetteddetail_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        arinvoicenetteddetail_sk,

        -- identifiers
        id,
        arinvoicenetteddetail_identity,
        invoice_id,
        owner_revenue_detail_id,
        voucher_id,
        well_id,

        -- dates
        accrual_date,
        netting_date,

        -- financial
        net_value_non_working,
        net_value_working,
        netted_amount,

        -- volumes
        net_volume_non_working,
        net_volume_working,

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
