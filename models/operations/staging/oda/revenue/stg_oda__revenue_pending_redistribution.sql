{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Revenue Pending Redistribution jobs.

    Source: ODA_REVENUEPENDINGREDISTRIBUTION (Estuary batch)
    Grain: One row per pending redistribution job (id)

    Notes:
    - Batch table — no CDC soft delete filtering needed (_meta/op is always 'c')
    - No deduplication needed — Estuary batch handles dedup at the connector level
    - FILTERPAYABLE and SUPPRESSGROSS confirmed native BOOLEAN in information_schema
    - CASHDATESOURCEID, DIVISIONOFINTERESTSOURCEID, RESETPAYMENTSTATUSID,
      REVENUEPENDINGREDISTRIBUTIONIDENTITY all NUMBER(38,0) → ::int
    - Timestamps are TIMESTAMP_LTZ in source → cast to ::timestamp_ntz per convention
    - _meta/op excluded — batch table, never contains 'd' operations
    - FLOW_DOCUMENT excluded — large JSON blob, not needed downstream
    - Renamed from stg_oda__revenuependingredistribution to snake_case in M2 sprint
    - Validated against information_schema.columns on 2026-02-20
#}

with

source as (
    select * from {{ source('oda', 'ODA_REVENUEPENDINGREDISTRIBUTION') }}
),

renamed as (
    select
        -- identifiers
        ID::varchar as id,
        REVENUEPENDINGREDISTRIBUTIONIDENTITY::int as revenue_pending_redistribution_identity,
        VOUCHERID::varchar as voucher_id,
        FILTERCOMPANYID::varchar as filter_company_id,
        FILTEROWNERID::varchar as filter_owner_id,
        FILTEROWNERGROUPID::varchar as filter_owner_group_id,
        FILTERPRODUCTID::varchar as filter_product_id,
        FILTERWELLID::varchar as filter_well_id,
        FILTERWELLGROUPID::varchar as filter_well_group_id,
        RESETSUSPENSECATEGORYID::varchar as reset_suspense_category_id,

        -- configuration
        CASHDATESOURCEID::int as cash_date_source_id,
        DIVISIONOFINTERESTSOURCEID::int as division_of_interest_source_id,
        RESETPAYMENTSTATUSID::int as reset_payment_status_id,

        -- filter dates
        FILTERFROMPRODUCTIONDATE::date as filter_from_production_date,
        FILTERTHRUPRODUCTIONDATE::date as filter_thru_production_date,

        -- flags (confirmed native BOOLEAN)
        FILTERPAYABLE::boolean as is_filter_payable,
        SUPPRESSGROSS::boolean as is_suppress_gross,

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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as revenue_pending_redistribution_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        revenue_pending_redistribution_sk,

        -- identifiers
        id,
        revenue_pending_redistribution_identity,
        voucher_id,
        filter_company_id,
        filter_owner_id,
        filter_owner_group_id,
        filter_product_id,
        filter_well_id,
        filter_well_group_id,
        reset_suspense_category_id,

        -- configuration
        cash_date_source_id,
        division_of_interest_source_id,
        reset_payment_status_id,

        -- filter dates
        filter_from_production_date,
        filter_thru_production_date,

        -- flags
        is_filter_payable,
        is_suppress_gross,

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
