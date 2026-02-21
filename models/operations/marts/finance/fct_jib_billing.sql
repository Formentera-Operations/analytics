{{
    config(
        materialized='table',
        tags=['marts', 'finance', 'jib'],
        cluster_by=['well_id', 'owner_id', 'accrual_date']
    )
}}

{#
    Mart: Joint Interest Billing Detail

    Purpose: Enriched JIB detail line items with dimension lookups for accounts,
    wells, owners, companies, and AFEs. Single source of truth for JIB reporting.

    Grain: One row per JIB detail line item (jibdetail.id)
    Row count: ~63M (CDC, soft deletes filtered in staging)

    CRITICAL: JIB and JIBDetail have NO direct FK relationship (parallel CDC views).
    Investigation confirmed ZERO DETAILID values match JIB.ID. This model uses
    JIBDetail as the sole source — it contains all FK columns needed.

    Enrichment joins (all LEFT JOIN):
    - dim_accounts → account_name, los_category
    - dim_wells → well_code, eid, well_name
    - dim_owners → owner_name, owner_code
    - dim_companies → company_name, company_code
    - dim_afes → afe_code, afe_name, afe_type_code
    - stg_oda__entity_type → entity_type_name

    Dependencies:
    - stg_oda__jibdetail (primary source)
    - dim_accounts, dim_wells, dim_owners, dim_companies, dim_afes
    - stg_oda__entity_type
#}

with

-- =============================================================================
-- Source: JIB Detail line items
-- =============================================================================
jib_details as (
    select * from {{ ref('stg_oda__jibdetail') }}
),

-- =============================================================================
-- Dimension lookups
-- =============================================================================
accounts as (
    select
        account_id,
        account_name,
        main_account,
        sub_account,
        is_los_account,
        los_category,
        los_section
    from {{ ref('dim_accounts') }}
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

owners as (
    select
        owner_id,
        owner_code,
        owner_name
    from {{ ref('dim_owners') }}
),

companies as (
    select
        company_id,
        company_code,
        company_name
    from {{ ref('dim_companies') }}
),

afes as (
    select
        afe_id,
        afe_code,
        afe_name,
        afe_type_code
    from {{ ref('dim_afes') }}
),

-- =============================================================================
-- Enriched output
-- =============================================================================
final as (
    select
        -- =================================================================
        -- Surrogate Key
        -- =================================================================
        {{ dbt_utils.generate_surrogate_key(['jd.id']) }} as jib_billing_sk,

        -- =================================================================
        -- Natural Key
        -- =================================================================
        jd.id,
        jd.jib_detail_identity,
        jd.detail_id,

        -- =================================================================
        -- Company
        -- =================================================================
        jd.company_id,
        c.company_code,
        c.company_name,

        -- =================================================================
        -- Owner
        -- =================================================================
        jd.owner_id,
        o.owner_code,
        o.owner_name,

        -- =================================================================
        -- Well
        -- =================================================================
        jd.well_id,
        w.well_code,
        w.well_name,
        we.eid,

        -- =================================================================
        -- Account
        -- =================================================================
        jd.account_id,
        a.account_name,
        a.main_account,
        a.sub_account,
        a.is_los_account,
        a.los_category,
        a.los_section,

        -- =================================================================
        -- AFE
        -- =================================================================
        jd.afe_id,
        af.afe_code,
        af.afe_name,
        af.afe_type_code,

        -- =================================================================
        -- Entity References
        -- =================================================================
        jd.entity_type_id,
        case jd.entity_type_id
            when 0 then 'Company'
            when 1 then 'Purchaser'
            when 3 then 'Owner'
            when 4 then 'Vendor'
            when 12 then 'None'
            when 13 then 'Operator'
        end as entity_type_name,
        jd.entity_company_id,
        jd.entity_purchaser_id,
        jd.entity_vendor_id,

        -- =================================================================
        -- Voucher / Invoice References
        -- =================================================================
        jd.voucher_id,
        jd.ar_invoice_id,
        jd.expense_deck_revision_id,

        -- =================================================================
        -- Dates
        -- =================================================================
        jd.accrual_date,
        jd.billed_date,
        jd.expense_date,

        -- =================================================================
        -- Financial
        -- =================================================================
        jd.billing_status_id,
        jd.expense_deck_interest,
        jd.gross_value,
        jd.net_value,

        -- =================================================================
        -- Flags
        -- =================================================================
        jd.is_include_in_accrual_report,
        jd.is_after_casing,

        -- =================================================================
        -- Suspense
        -- =================================================================
        jd.billing_suspense_category_id,

        -- =================================================================
        -- Redistribution
        -- =================================================================
        jd.pending_redistribution_id,
        jd.redistribution_voucher_id,

        -- =================================================================
        -- Descriptive
        -- =================================================================
        jd.description,
        jd.reference,

        -- =================================================================
        -- Audit
        -- =================================================================
        jd._loaded_at,
        jd._flow_published_at

    from jib_details jd
    left join accounts a on jd.account_id = a.account_id
    left join wells w on jd.well_id = w.well_id
    left join well_eids we on jd.well_id = we.oda_well_id
    left join owners o on jd.owner_id = o.owner_id
    left join companies c on jd.company_id = c.company_id
    left join afes af on jd.afe_id = af.afe_id
)

select * from final
