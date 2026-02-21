{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns',
        incremental_strategy='merge',
        cluster_by=['well_id', 'production_date', 'owner_id'],
        tags=['marts', 'finance', 'revenue'],
        snowflake_warehouse=set_warehouse_size('M') if target.name in ['prod', 'ci'] else target.warehouse,
    )
}}

{#
    Mart: Revenue Distribution Detail

    Purpose: Enriched owner revenue distribution line items with dimension lookups
    for owners, wells, products, purchasers, companies, interest types, payment
    status, and suspense categories. Single source of truth for revenue reporting.

    Grain: One row per owner revenue detail line item (id). ~61M rows.
    Incremental: merge on id, _flow_published_at watermark.
    Clustered by well_id, production_date, owner_id.

    Dependencies:
    - stg_oda__owner_revenue_detail_v2 (primary source)
    - dim_owners, dim_wells, dim_purchasers, dim_companies
    - stg_oda__product, stg_oda__interest_type
    - stg_oda__revenue_payment_status, stg_oda__revenue_suspense_category
    - well_360 (EID resolution)
#}

with

-- =============================================================================
-- Source: Owner revenue distribution line items
-- =============================================================================
revenue_details as (
    select * from {{ ref('stg_oda__owner_revenue_detail_v2') }}
    {% if is_incremental() %}
        where _flow_published_at > (
            select coalesce(max(_flow_published_at), '1900-01-01'::timestamp_tz)
            from {{ this }}
        )
    {% endif %}
),

-- =============================================================================
-- Dimension lookups
-- =============================================================================
owners as (
    select
        owner_id,
        owner_code,
        owner_name
    from {{ ref('dim_owners') }}
),

wells as (
    select
        well_id,
        well_code,
        well_name
    from {{ ref('dim_wells') }}
),

well_eids as (
    select
        oda_well_id,
        eid
    from {{ ref('well_360') }}
    where oda_well_id is not null
),

products as (
    select
        id as product_id,
        code as product_code,
        name as product_name
    from {{ ref('stg_oda__product') }}
),

interest_types as (
    select
        interest_type_identity as interest_type_id,
        code as interest_type_code,
        name as interest_type_name
    from {{ ref('stg_oda__interest_type') }}
),

purchasers as (
    select
        purchaser_id,
        purchaser_code,
        purchaser_name
    from {{ ref('dim_purchasers') }}
),

companies as (
    select
        company_id,
        company_code,
        company_name
    from {{ ref('dim_companies') }}
),

payment_statuses as (
    select
        id as payment_status_id,
        name as payment_status_name
    from {{ ref('stg_oda__revenue_payment_status') }}
),

suspense_categories as (
    select
        id as suspense_category_id,
        name as suspense_category_name
    from {{ ref('stg_oda__revenue_suspense_category') }}
),

-- =============================================================================
-- Enriched output
-- =============================================================================
final as (
    select
        -- =================================================================
        -- Surrogate Key
        -- =================================================================
        {{ dbt_utils.generate_surrogate_key(['rd.id']) }} as revenue_distribution_sk,

        -- =================================================================
        -- Natural Key
        -- =================================================================
        rd.id,
        rd.owner_revenue_detail_identity,

        -- =================================================================
        -- Company
        -- =================================================================
        rd.company_id,
        c.company_code,
        c.company_name,

        -- =================================================================
        -- Owner
        -- =================================================================
        rd.owner_id,
        o.owner_code,
        o.owner_name,

        -- =================================================================
        -- Well
        -- =================================================================
        rd.well_id,
        w.well_code,
        w.well_name,
        we.eid,

        -- =================================================================
        -- Product
        -- =================================================================
        rd.product_id,
        p.product_code,
        p.product_name,

        -- =================================================================
        -- Interest
        -- =================================================================
        rd.interest_type_id,
        it.interest_type_code,
        it.interest_type_name,
        rd.custom_interest_type_id,

        -- =================================================================
        -- Purchaser
        -- =================================================================
        rd.purchaser_id,
        pu.purchaser_code,
        pu.purchaser_name,

        -- =================================================================
        -- Revenue Deck / Redistribution References
        -- =================================================================
        rd.revenue_deck_id,
        rd.revenue_deck_revision_id,
        rd.redistribution_id,
        rd.pending_redistribution_id,
        rd.voucher_id,
        rd.purchaser_receipt_id,
        rd.currency_id,
        rd.gross_event_id,
        rd.import_data_id,
        rd.memo_company_id,

        -- =================================================================
        -- Financial
        -- =================================================================
        rd.net_value,
        rd.net_volume,
        rd.netted_amount,
        rd.paid_amount,
        rd.next_payment,
        rd.decimal_interest,
        rd.btu_factor,

        -- =================================================================
        -- Counts / Status
        -- =================================================================
        rd.sign_multiplier,
        rd.check_stub_count,
        rd.netted_detail_count,
        rd.open_netted_detail_count,
        rd.payment_status_id,
        ps.payment_status_name,
        rd.statement_status_id,

        -- =================================================================
        -- Suspense
        -- =================================================================
        rd.suspense_category_id,
        sc.suspense_category_name,
        rd.pending_suspense_category_id,

        -- =================================================================
        -- Dates
        -- =================================================================
        rd.accrual_date,
        rd.production_date,

        -- =================================================================
        -- Flags
        -- =================================================================
        rd.is_duplicate_gross,
        rd.is_historical,
        rd.is_include_in_accrual_report,
        rd.is_memo_also_code_missing,
        rd.is_suspense_category_processed,

        -- =================================================================
        -- Audit
        -- =================================================================
        rd.created_at,
        rd.updated_at,
        rd.record_inserted_at,
        rd.record_updated_at,

        -- =================================================================
        -- Metadata
        -- =================================================================
        current_timestamp() as _loaded_at,
        rd._flow_published_at

    from revenue_details rd
    left join owners o on rd.owner_id = o.owner_id
    left join wells w on rd.well_id = w.well_id
    left join well_eids we on rd.well_id = we.oda_well_id
    left join products p on rd.product_id = p.product_id
    left join interest_types it on rd.interest_type_id = it.interest_type_id
    left join purchasers pu on rd.purchaser_id = pu.purchaser_id
    left join companies c on rd.company_id = c.company_id
    left join payment_statuses ps on rd.payment_status_id = ps.payment_status_id
    left join suspense_categories sc on rd.suspense_category_id = sc.suspense_category_id
)

select * from final
