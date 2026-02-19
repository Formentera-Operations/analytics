{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Owner Revenue Distribution Line Items V2.

    Source: ODA_OWNERREVENUEDETAIL_V2 (Estuary batch, ~61M rows)
    Grain: One row per owner revenue distribution line item (id)

    Notes:
    - Batch table — no CDC soft delete filtering needed (_meta/op is always 'c')
    - No deduplication needed — Estuary batch handles dedup at the connector level
    - All boolean columns confirmed as native BOOLEAN type in information_schema
    - Timestamps are TIMESTAMP_LTZ in source → cast to ::timestamp_ntz per convention
    - _meta/op excluded — batch table, never contains 'd' operations
    - FLOW_DOCUMENT excluded — large JSON blob, not needed downstream
    - Validated against information_schema.columns on 2026-02-19
#}

with

source as (
    select * from {{ source('oda', 'ODA_OWNERREVENUEDETAIL_V2') }}
),

renamed as (
    select 
        -- identifiers
        ID::varchar                                 as id,
        OWNERREVENUEDETAILIDENTITY::int             as owner_revenue_detail_identity,
        OWNERID::varchar                            as owner_id,
        COMPANYID::varchar                          as company_id,
        WELLID::varchar                             as well_id,
        PRODUCTID::varchar                          as product_id,
        INTERESTTYPEID::int                         as interest_type_id,
        CUSTOMINTERESTTYPEID::varchar               as custom_interest_type_id,
        VOUCHERID::varchar                          as voucher_id,
        CURRENCYID::varchar                         as currency_id,
        PURCHASERID::varchar                        as purchaser_id,
        PURCHASERRECEIPTID::varchar                 as purchaser_receipt_id,
        REVENUEDECKID::varchar                      as revenue_deck_id,
        REVENUEDECKREVISIONID::varchar              as revenue_deck_revision_id,
        REDISTRIBUTIONID::varchar                   as redistribution_id,
        PENDINGREDISTRIBUTIONID::varchar            as pending_redistribution_id,
        SUSPENSECATEGORYID::varchar                 as suspense_category_id,
        PENDINGSUSPENSECATEGORYID::varchar          as pending_suspense_category_id,
        IMPORTDATAID::varchar                       as import_data_id,
        GROSSEVENTID::varchar                       as gross_event_id,
        MEMOCOMPANYID::varchar                      as memo_company_id,

        -- financial
        NETVALUE::float                             as net_value,
        NETVOLUME::float                            as net_volume,
        NETTEDAMOUNT::float                         as netted_amount,
        PAIDAMOUNT::float                           as paid_amount,
        NEXTPAYMENT::float                          as next_payment,
        DECIMALINTEREST::float                      as decimal_interest,
        BTUFACTOR::float                            as btu_factor,

        -- counts / status
        SIGNMULTIPLIER::int                         as sign_multiplier,
        CHECKSTUBCOUNT::int                         as check_stub_count,
        NETTEDDETAILCOUNT::int                      as netted_detail_count,
        OPENNETTEDDETAILCOUNT::int                  as open_netted_detail_count,
        PAYMENTSTATUSID::int                        as payment_status_id,
        STATEMENTSTATUSID::int                      as statement_status_id,

        -- dates
        ACCRUALDATE::date                           as accrual_date,
        PRODUCTIONDATE::date                        as production_date,

        -- flags (all confirmed native BOOLEAN)
        DUPLICATEGROSS::boolean                     as is_duplicate_gross,
        HISTORICAL::boolean                         as is_historical,
        INCLUDEINACCRUALREPORT::boolean             as is_include_in_accrual_report,
        MEMOALSOCODEMISSING::boolean                as is_memo_also_code_missing,
        SUSPENSECATEGORYPROCESSED::boolean          as is_suspense_category_processed,

        -- audit
        CREATEDATE::timestamp_ntz                   as created_at,
        CREATEEVENTID::varchar                      as create_event_id,
        UPDATEDATE::timestamp_ntz                   as updated_at,
        UPDATEEVENTID::varchar                      as update_event_id,
        RECORDINSERTDATE::timestamp_ntz             as record_inserted_at,
        RECORDUPDATEDATE::timestamp_ntz             as record_updated_at,

        -- ingestion metadata
        FLOW_PUBLISHED_AT::timestamp_tz             as _flow_published_at

    from source
),

filtered as (
    select *
    from renamed
    where id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['id']) }} as owner_revenue_detail_v2_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        owner_revenue_detail_v2_sk,

        -- identifiers
        id,
        owner_revenue_detail_identity,
        owner_id,
        company_id,
        well_id,
        product_id,
        interest_type_id,
        custom_interest_type_id,
        voucher_id,
        currency_id,
        purchaser_id,
        purchaser_receipt_id,
        revenue_deck_id,
        revenue_deck_revision_id,
        redistribution_id,
        pending_redistribution_id,
        suspense_category_id,
        pending_suspense_category_id,
        import_data_id,
        gross_event_id,
        memo_company_id,

        -- financial
        net_value,
        net_volume,
        netted_amount,
        paid_amount,
        next_payment,
        decimal_interest,
        btu_factor,

        -- counts / status
        sign_multiplier,
        check_stub_count,
        netted_detail_count,
        open_netted_detail_count,
        payment_status_id,
        statement_status_id,

        -- dates
        accrual_date,
        production_date,

        -- flags
        is_duplicate_gross,
        is_historical,
        is_include_in_accrual_report,
        is_memo_also_code_missing,
        is_suspense_category_processed,

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
