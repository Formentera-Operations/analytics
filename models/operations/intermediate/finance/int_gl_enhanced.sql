{{
    config(
        materialized='incremental',
        unique_key='gl_id',
        on_schema_change='sync_all_columns',
        incremental_strategy='merge',
        cluster_by=['company_code', 'journal_date', 'main_account', 'sub_account'],
        tags=['intermediate', 'finance', 'gl'],
        snowflake_warehouse=set_warehouse_size('M') if target.name in ['prod', 'ci'] else target.warehouse,
    )
}}

{#
    Intermediate model: Enhanced General Ledger
    
    Purpose: Enriches GL entries with dimensional attributes for reporting
    Grain: One row per GL entry (gl_id)
    
    Key enrichments:
    - Company and account details
    - Polymorphic location resolution (Company/Owner/Purchaser/Vendor/Well)
    - Polymorphic entity resolution
    - AFE classification
    - Voucher and posting information
    - Revenue/Expense deck details
    - Well search key from userfields
    
    Incremental strategy:
    - Uses _loaded_at watermark from staging model
    - Note: Late-arriving dimension updates require periodic full refresh
    
    Dependencies:
    - stg_oda__gl (source)
    - Multiple dimension tables (accounts, companies, wells, etc.)
#}

with source_gl as (
    select * from {{ ref('stg_oda__gl') }}
    {% if is_incremental() %}
        where _loaded_at > (select max(_loaded_at) from {{ this }})
    {% endif %}
),

-- =============================================================================
-- Dimension CTEs
-- =============================================================================

companies as (
    select * from {{ ref('stg_oda__company_v2') }}
),

accounts as (
    select * from {{ ref('stg_oda__account_v2') }}
),

vouchers as (
    select * from {{ ref('stg_oda__voucher_v2') }}
),

wells as (
    select * from {{ ref('stg_oda__wells') }}
),

entities as (
    select * from {{ ref('stg_oda__entity_v2') }}
),

owners as (
    select * from {{ ref('stg_oda__owner_v2') }}
),

purchasers as (
    select * from {{ ref('stg_oda__purchaser_v2') }}
),

vendors as (
    select * from {{ ref('stg_oda__vendor_v2') }}
),

afes as (
    select * from {{ ref('stg_oda__afe_v2') }}
),

source_modules as (
    select * from {{ ref('stg_oda__source_module') }}
),

payment_types as (
    select * from {{ ref('stg_oda__payment_type') }}
),

recon_types as (
    select * from {{ ref('stg_oda__gl_reconciliation_type') }}
),

-- Revenue deck chain
rev_deck_revisions as (
    select * from {{ ref('stg_oda__revenue_deck_revision') }}
),

rev_decks as (
    select * from {{ ref('stg_oda__revenue_deck_v2') }}
),

-- Expense deck chain
exp_deck_revisions as (
    select * from {{ ref('stg_oda__expense_deck_revision') }}
),

exp_decks as (
    select * from {{ ref('stg_oda__expense_deck_v2') }}
),

exp_deck_sets as (
    select * from {{ ref('stg_oda__expense_deck_set') }}
),

-- =============================================================================
-- Userfields: Search key for wells
-- ENTITYTYPEID = 2 filters to wells only (avoids owner/purchaser/vendor userfields)
-- =============================================================================

userfields_search_key as (
    select
        cast(id as varchar) as well_id,
        user_field_value_string as search_key
    from {{ ref('stg_oda__userfield') }}
    where
        user_field_name = 'UF-SEARCH KEY'
        and entity_type_id = 2  -- Wells only
    qualify row_number() over (
        partition by id
        order by _flow_published_at desc
    ) = 1
),

-- =============================================================================
-- Polymorphic Location Resolution (consolidated)
-- Note: Explicit VARCHAR casts required - entity.code is NUMBER, others are VARCHAR
-- =============================================================================

location_enriched as (
    select
        gld.id as gl_id,

        -- Determine location type based on which ID is populated
        case
            when gld.location_company_id is not null then 'Company'
            when gld.location_owner_id is not null then 'Owner'
            when gld.location_purchaser_id is not null then 'Purchaser'
            when gld.location_vendor_id is not null then 'Vendor'
            when gld.location_well_id is not null then 'Well'
        end as location_type,

        -- Resolve code with explicit casts (entity.code is NUMBER)
        coalesce(
            cast(loc_company.code as varchar),
            cast(loc_owner_entity.code as varchar),
            cast(loc_purchaser_entity.code as varchar),
            cast(loc_vendor_entity.code as varchar),
            cast(loc_well.code as varchar)
        ) as location_code,

        -- Resolve name with explicit casts for consistency
        coalesce(
            cast(loc_company.name as varchar),
            cast(loc_owner_entity.name as varchar),
            cast(loc_purchaser_entity.name as varchar),
            cast(loc_vendor_entity.name as varchar),
            cast(loc_well.name as varchar)
        ) as location_name

    from source_gl as gld

    -- Company location
    left join companies as loc_company
        on gld.location_company_id = loc_company.id

    -- Owner location (through entity)
    left join owners as loc_owner
        on gld.location_owner_id = loc_owner.id
    left join entities as loc_owner_entity
        on loc_owner.entity_id = loc_owner_entity.id

    -- Purchaser location (through entity)
    left join purchasers as loc_purchaser
        on gld.location_purchaser_id = loc_purchaser.id
    left join entities as loc_purchaser_entity
        on loc_purchaser.entity_id = loc_purchaser_entity.id

    -- Vendor location (through entity)
    left join vendors as loc_vendor
        on gld.location_vendor_id = loc_vendor.id
    left join entities as loc_vendor_entity
        on loc_vendor.entity_id = loc_vendor_entity.id

    -- Well location
    left join wells as loc_well
        on gld.location_well_id = loc_well.id
),

-- =============================================================================
-- Polymorphic Entity Resolution (consolidated)
-- Note: Explicit VARCHAR casts required - entity.code is NUMBER, others are VARCHAR
-- =============================================================================

entity_enriched as (
    select
        gld.id as gl_id,

        -- Determine entity type
        ent_owner.entity_id as owner_entity_id,

        -- Separate entity IDs for direct dimension joins
        ent_vendor.entity_id as vendor_entity_id,
        ent_purchaser.entity_id as purchaser_entity_id,
        case
            when gld.entity_company_id is not null then 'Company'
            when gld.entity_owner_id is not null then 'Owner'
            when gld.entity_purchaser_id is not null then 'Purchaser'
            when gld.entity_vendor_id is not null then 'Vendor'
        end as entity_type,

        -- Entity code (no prefix - matches dimension codes)
        coalesce(
            cast(ent_company.code as varchar),
            cast(ent_owner_entity.code as varchar),
            cast(ent_purchaser_entity.code as varchar),
            cast(ent_vendor_entity.code as varchar)
        ) as entity_code,

        -- Entity name
        coalesce(
            cast(ent_company.name as varchar),
            cast(ent_owner_entity.name as varchar),
            cast(ent_purchaser_entity.name as varchar),
            cast(ent_vendor_entity.name as varchar)
        ) as entity_name

    from source_gl as gld

    -- Company entity
    left join companies as ent_company
        on gld.entity_company_id = ent_company.id

    -- Owner entity (through entity)
    left join owners as ent_owner
        on gld.entity_owner_id = ent_owner.id
    left join entities as ent_owner_entity
        on ent_owner.entity_id = ent_owner_entity.id

    -- Purchaser entity (through entity)
    left join purchasers as ent_purchaser
        on gld.entity_purchaser_id = ent_purchaser.id
    left join entities as ent_purchaser_entity
        on ent_purchaser.entity_id = ent_purchaser_entity.id

    -- Vendor entity (through entity)
    left join vendors as ent_vendor
        on gld.entity_vendor_id = ent_vendor.id
    left join entities as ent_vendor_entity
        on ent_vendor.entity_id = ent_vendor_entity.id
),

-- =============================================================================
-- Final Select
-- =============================================================================

final as (
    select
        -- Surrogate key and metadata
        gld.id as gl_id,
        gld._loaded_at,
        gld.created_at,
        gld.updated_at,
        c.code as company_code,

        -- Company
        c.name as company_name,
        acct.id as account_id,

        -- Account
        acct.main_account,
        acct.sub_account,
        acct.account_name as account_name,
        afes.id as afe_id,

        -- AFE classification
        cast(afes.code as varchar) as afe_code,
        cast(afes.afe_type_id as varchar) as afe_type_id,
        cast(afes.afe_type_code as varchar) as afe_type_code,
        cast(afes.afe_type_label as varchar) as afe_type_label,
        cast(afes.afe_type_full_name as varchar) as afe_type_full_name,
        loc.location_type,

        -- Location (from consolidated CTE)
        loc.location_code,
        loc.location_name,
        ent.entity_type,

        -- Entity (from consolidated CTE)
        ent.owner_entity_id,
        ent.vendor_entity_id,
        ent.purchaser_entity_id,
        ent.entity_code,
        ent.entity_name,
        wells.id as well_id,

        -- Well (direct)
        cast(wells.code as varchar) as well_code,
        cast(wells.name as varchar) as well_name,
        wells.property_reference_code as op_ref,
        uf.search_key,
        gld.is_posted,

        -- Posting status
        vouchers.id as voucher_id,
        cast(vouchers.code as varchar) as voucher_code,
        cast(vouchers.voucher_type_id as varchar) as voucher_type_id,
        vouchers.posted_date as posted_at,
        gld.journal_date,
        gld.accrual_date,

        -- Journal date (primary)
        gld.cash_date,
        cast(gld.source_module_code as varchar) as source_module_code,
        cast(source_modules.name as varchar) as source_module_name,

        -- Accrual date
        cast(payment_types.code as varchar) as payment_type_code,
        cast(gld.reference as varchar) as reference,
        cast(gld.description as varchar) as gl_description,

        -- Cash date
        gld.gross_value as gross_amount,
        gld.net_value as net_amount,
        gld.gross_volume,

        -- Source tracking
        gld.net_volume,
        gld.currency_id,
        gld.is_currency_defaulted,
        gld.is_include_in_journal_report,
        gld.is_present_in_journal_balance,

        -- Financial values (preserving decimal precision)
        gld.is_include_in_cash_report,
        gld.is_present_in_cash_balance,
        gld.is_include_in_accrual_report,
        gld.is_present_in_accrual_balance,
        cast(rev_deck_revisions.revision_number as varchar) as revenue_deck_revision,
        rev_decks.effective_date as revenue_deck_effective_date,

        -- Report inclusion flags
        cast(exp_deck_sets.code as varchar) as expense_deck_set_code,
        cast(exp_deck_revisions.revision_number as varchar) as expense_deck_revision,
        exp_decks.effective_date as expense_deck_effective_date,
        exp_deck_revisions.total_interest_actual as expense_deck_interest_total,
        cast(recon_types.code as varchar) as reconciliation_type_code,
        gld.is_reconciled,

        -- Revenue deck
        gld.is_reconciled_trial,
        gld.is_generated_entry,

        -- Expense deck
        gld.is_allocation_parent,
        gld.is_allocation_generated,
        gld.allocation_parent_id,
        cast(gld.entry_group as varchar) as entry_group,

        -- Reconciliation
        gld.ordinal as entry_sequence,
        cast(convert_timezone('UTC', 'America/Chicago', current_timestamp()) as timestamp_tz) as _last_refresh_at,
        cast(convert_timezone('UTC', 'America/Chicago', vouchers.posted_date) as timestamp_tz) as posted_at_cst,

        -- Entry metadata
        cast(date_trunc('month', gld.journal_date) as date) as journal_month_start,
        cast(extract(year from gld.journal_date) as int) as journal_year,
        cast(date_trunc('month', gld.accrual_date) as date) as accrual_month_start,
        cast(extract(year from gld.accrual_date) as int) as accrual_year,
        cast(date_trunc('month', gld.cash_date) as date) as cash_month_start,
        cast(extract(year from gld.cash_date) as int) as cash_year

    from source_gl as gld

    -- Core dimensions
    left join companies as c
        on gld.company_id = c.id
    left join accounts as acct
        on gld.account_id = acct.id
    left join vouchers
        on gld.voucher_id = vouchers.id
    left join wells
        on gld.well_id = wells.id
    left join afes
        on gld.afe_id = afes.id

    -- Enriched polymorphic data
    left join location_enriched as loc
        on gld.id = loc.gl_id
    left join entity_enriched as ent
        on gld.id = ent.gl_id

    -- Reference dimensions
    left join source_modules
        on gld.source_module_code = source_modules.code
    left join payment_types
        on gld.payment_type_id = payment_types.id
    left join recon_types
        on gld.reconciliation_type_id = recon_types.id

    -- Userfields (search key for wells)
    left join userfields_search_key as uf
        on cast(gld.well_id as varchar) = uf.well_id

    -- Revenue deck chain
    left join rev_deck_revisions
        on gld.source_revenue_deck_revision_id = rev_deck_revisions.id
    left join rev_decks
        on rev_deck_revisions.deck_id = rev_decks.id

    -- Expense deck chain
    left join exp_deck_revisions
        on gld.source_expense_deck_revision_id = exp_deck_revisions.id
    left join exp_decks
        on exp_deck_revisions.deck_id = exp_decks.id
    left join exp_deck_sets
        on exp_decks.deck_set_id = exp_deck_sets.id
)

select * from final
