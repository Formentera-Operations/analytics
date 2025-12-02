{{
    config(
        materialized='incremental',
        unique_key='gl_id',
        on_schema_change='sync_all_columns',
        incremental_strategy='merge',
        cluster_by=['company_code', 'journal_date', 'main_account', 'sub_account'],
        tags=['intermediate', 'finance', 'gl']
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

WITH source_gl AS (
    SELECT * FROM {{ ref('stg_oda__gl') }}
    {% if is_incremental() %}
    WHERE _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
    {% endif %}
),

-- =============================================================================
-- Dimension CTEs
-- =============================================================================

companies AS (
    SELECT * FROM {{ ref('stg_oda__company_v2') }}
),

accounts AS (
    SELECT * FROM {{ ref('stg_oda__account_v2') }}
),

vouchers AS (
    SELECT * FROM {{ ref('stg_oda__voucher_v2') }}
),

wells AS (
    SELECT * FROM {{ ref('stg_oda__wells') }}
),

entities AS (
    SELECT * FROM {{ ref('stg_oda__entity_v2') }}
),

owners AS (
    SELECT * FROM {{ ref('stg_oda__owner_v2') }}
),

purchasers AS (
    SELECT * FROM {{ ref('stg_oda__purchaser_v2') }}
),

vendors AS (
    SELECT * FROM {{ ref('stg_oda__vendor_v2') }}
),

afes AS (
    SELECT * FROM {{ ref('stg_oda__afe_v2') }}
),

source_modules AS (
    SELECT * FROM {{ ref('stg_oda__source_module') }}
),

payment_types AS (
    SELECT * FROM {{ ref('stg_oda__payment_type') }}
),

recon_types AS (
    SELECT * FROM {{ ref('stg_oda__gl_reconciliation_type') }}
),

-- Revenue deck chain
rev_deck_revisions AS (
    SELECT * FROM {{ ref('stg_oda__revenue_deck_revision') }}
),

rev_decks AS (
    SELECT * FROM {{ ref('stg_oda__revenue_deck_v2') }}
),

-- Expense deck chain
exp_deck_revisions AS (
    SELECT * FROM {{ ref('stg_oda__expense_deck_revision') }}
),

exp_decks AS (
    SELECT * FROM {{ ref('stg_oda__expense_deck_v2') }}
),

exp_deck_sets AS (
    SELECT * FROM {{ ref('stg_oda__expense_deck_set') }}
),

-- =============================================================================
-- Userfields: Search key for wells
-- ENTITYTYPEID = 2 filters to wells only (avoids owner/purchaser/vendor userfields)
-- =============================================================================

userfields_search_key AS (
    SELECT 
        CAST(ID AS VARCHAR) AS well_id,
        USERFIELDVALUESTRING AS search_key
    FROM {{ ref('stg_oda__userfield') }}
    WHERE USERFIELDNAME = 'UF-SEARCH KEY'
      AND ENTITYTYPEID = 2  -- Wells only
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ID ORDER BY USERFIELDIDENTITY DESC) = 1
),

-- =============================================================================
-- Polymorphic Location Resolution (consolidated)
-- Note: Explicit VARCHAR casts required - entity.code is NUMBER, others are VARCHAR
-- =============================================================================

location_enriched AS (
    SELECT
        gld.id AS gl_id,
        
        -- Determine location type based on which ID is populated
        CASE
            WHEN gld.location_company_id IS NOT NULL THEN 'Company'
            WHEN gld.location_owner_id IS NOT NULL THEN 'Owner'
            WHEN gld.location_purchaser_id IS NOT NULL THEN 'Purchaser'
            WHEN gld.location_vendor_id IS NOT NULL THEN 'Vendor'
            WHEN gld.location_well_id IS NOT NULL THEN 'Well'
        END AS location_type,
        
        -- Resolve code with explicit casts (entity.code is NUMBER)
        COALESCE(
            CAST(loc_company.code AS VARCHAR),
            CAST(loc_owner_entity.code AS VARCHAR),
            CAST(loc_purchaser_entity.code AS VARCHAR),
            CAST(loc_vendor_entity.code AS VARCHAR),
            CAST(loc_well.code AS VARCHAR)
        ) AS location_code,
        
        -- Resolve name with explicit casts for consistency
        COALESCE(
            CAST(loc_company.name AS VARCHAR),
            CAST(loc_owner_entity.name AS VARCHAR),
            CAST(loc_purchaser_entity.name AS VARCHAR),
            CAST(loc_vendor_entity.name AS VARCHAR),
            CAST(loc_well.name AS VARCHAR)
        ) AS location_name

    FROM source_gl AS gld
    
    -- Company location
    LEFT JOIN companies AS loc_company
        ON gld.location_company_id = loc_company.id
    
    -- Owner location (through entity)
    LEFT JOIN owners AS loc_owner
        ON gld.location_owner_id = loc_owner.id
    LEFT JOIN entities AS loc_owner_entity
        ON loc_owner.entity_id = loc_owner_entity.id
    
    -- Purchaser location (through entity)
    LEFT JOIN purchasers AS loc_purchaser
        ON gld.location_purchaser_id = loc_purchaser.id
    LEFT JOIN entities AS loc_purchaser_entity
        ON loc_purchaser.entity_id = loc_purchaser_entity.id
    
    -- Vendor location (through entity)
    LEFT JOIN vendors AS loc_vendor
        ON gld.location_vendor_id = loc_vendor.id
    LEFT JOIN entities AS loc_vendor_entity
        ON loc_vendor.entity_id = loc_vendor_entity.id
    
    -- Well location
    LEFT JOIN wells AS loc_well
        ON gld.location_well_id = loc_well.id
),

-- =============================================================================
-- Polymorphic Entity Resolution (consolidated)
-- Note: Explicit VARCHAR casts required - entity.code is NUMBER, others are VARCHAR
-- =============================================================================

entity_enriched AS (
    SELECT
        gld.id AS gl_id,
        
        -- Determine entity type
        CASE
            WHEN gld.entity_company_id IS NOT NULL THEN 'Company'
            WHEN gld.entity_owner_id IS NOT NULL THEN 'Owner'
            WHEN gld.entity_purchaser_id IS NOT NULL THEN 'Purchaser'
            WHEN gld.entity_vendor_id IS NOT NULL THEN 'Vendor'
        END AS entity_type,
        
        -- Separate entity IDs for direct dimension joins
        ent_owner.entity_id AS owner_entity_id,
        ent_vendor.entity_id AS vendor_entity_id,
        ent_purchaser.entity_id AS purchaser_entity_id,
        
        -- Entity code (no prefix - matches dimension codes)
        COALESCE(
            CAST(ent_company.code AS VARCHAR),
            CAST(ent_owner_entity.code AS VARCHAR),
            CAST(ent_purchaser_entity.code AS VARCHAR),
            CAST(ent_vendor_entity.code AS VARCHAR)
        ) AS entity_code,
        
        -- Entity name
        COALESCE(
            CAST(ent_company.name AS VARCHAR),
            CAST(ent_owner_entity.name AS VARCHAR),
            CAST(ent_purchaser_entity.name AS VARCHAR),
            CAST(ent_vendor_entity.name AS VARCHAR)
        ) AS entity_name

    FROM source_gl AS gld
    
    -- Company entity
    LEFT JOIN companies AS ent_company
        ON gld.entity_company_id = ent_company.id
    
    -- Owner entity (through entity)
    LEFT JOIN owners AS ent_owner
        ON gld.entity_owner_id = ent_owner.id
    LEFT JOIN entities AS ent_owner_entity
        ON ent_owner.entity_id = ent_owner_entity.id
    
    -- Purchaser entity (through entity)
    LEFT JOIN purchasers AS ent_purchaser
        ON gld.entity_purchaser_id = ent_purchaser.id
    LEFT JOIN entities AS ent_purchaser_entity
        ON ent_purchaser.entity_id = ent_purchaser_entity.id
    
    -- Vendor entity (through entity)
    LEFT JOIN vendors AS ent_vendor
        ON gld.entity_vendor_id = ent_vendor.id
    LEFT JOIN entities AS ent_vendor_entity
        ON ent_vendor.entity_id = ent_vendor_entity.id
),

-- =============================================================================
-- Final Select
-- =============================================================================

final AS (
    SELECT 
        -- Surrogate key and metadata
        gld.id AS gl_id,
        CONVERT_TIMEZONE('UTC', 'America/Chicago', CURRENT_TIMESTAMP())::TIMESTAMP_TZ AS _last_refresh_at,
        gld._loaded_at,
        gld.created_at,
        gld.updated_at,
        
        -- Company
        c.code AS company_code,
        c.name AS company_name,
        
        -- Account
        acct.id AS account_id,
        acct.main_account,
        acct.sub_account,
        acct.name AS account_name,
        
        -- AFE classification
        afes.id AS afe_id,
        CAST(afes.code AS VARCHAR) AS afe_code,
        CAST(afes.afe_type_id AS VARCHAR) AS afe_type_id,
        CAST(afes.afe_type_code AS VARCHAR) AS afe_type_code,
        CAST(afes.afe_type_label AS VARCHAR) AS afe_type_label,
        CAST(afes.afe_type_full_name AS VARCHAR) AS afe_type_full_name,
        
        -- Location (from consolidated CTE)
        loc.location_type,
        loc.location_code,
        loc.location_name,
        
        -- Entity (from consolidated CTE)
        ent.entity_type,
        ent.owner_entity_id,
        ent.vendor_entity_id,
        ent.purchaser_entity_id,
        ent.entity_code,
        ent.entity_name,
        
        -- Well (direct)
        wells.id AS well_id,
        CAST(wells.code AS VARCHAR) AS well_code,
        CAST(wells.name AS VARCHAR) AS well_name,
        wells.property_reference_code AS op_ref,
        uf.search_key,
        
        -- Posting status
        gld.is_posted,
        vouchers.id AS voucher_id,
        CAST(vouchers.code AS VARCHAR) AS voucher_code,
        CAST(vouchers.voucher_type_id AS VARCHAR) AS voucher_type_id,
        vouchers.posted_date AS posted_at,
        CONVERT_TIMEZONE('UTC', 'America/Chicago', vouchers.posted_date)::TIMESTAMP_TZ AS posted_at_cst,
        
        -- Journal date (primary)
        gld.journal_date,
        DATE_TRUNC('month', gld.journal_date)::DATE AS journal_month_start,
        EXTRACT(YEAR FROM gld.journal_date)::INT AS journal_year,
        
        -- Accrual date
        gld.accrual_date,
        DATE_TRUNC('month', gld.accrual_date)::DATE AS accrual_month_start,
        EXTRACT(YEAR FROM gld.accrual_date)::INT AS accrual_year,
        
        -- Cash date
        gld.cash_date,
        DATE_TRUNC('month', gld.cash_date)::DATE AS cash_month_start,
        EXTRACT(YEAR FROM gld.cash_date)::INT AS cash_year,
        
        -- Source tracking
        CAST(gld.source_module_code AS VARCHAR) AS source_module_code,
        CAST(source_modules.name AS VARCHAR) AS source_module_name,
        CAST(payment_types.code AS VARCHAR) AS payment_type_code,
        CAST(gld.reference AS VARCHAR) AS reference,
        CAST(gld.description AS VARCHAR) AS gl_description,
        
        -- Financial values (preserving decimal precision)
        gld.gross_value AS gross_amount,
        gld.net_value AS net_amount,
        gld.gross_volume,
        gld.net_volume,
        gld.currency_id,
        gld.is_currency_missing,
        
        -- Report inclusion flags
        gld.is_include_in_journal_report,
        gld.is_present_in_journal_balance,
        gld.is_include_in_cash_report,
        gld.is_present_in_cash_balance,
        gld.is_include_in_accrual_report,
        gld.is_present_in_accrual_balance,
        
        -- Revenue deck
        CAST(rev_deck_revisions.revision_number AS VARCHAR) AS revenue_deck_revision,
        rev_decks.effective_date AS revenue_deck_effective_date,
        
        -- Expense deck
        CAST(exp_deck_sets.code AS VARCHAR) AS expense_deck_set_code,
        CAST(exp_deck_revisions.revision_number AS VARCHAR) AS expense_deck_revision,
        exp_decks.effective_date AS expense_deck_effective_date,
        exp_deck_revisions.total_interest_actual AS expense_deck_interest_total,
        
        -- Reconciliation
        CAST(recon_types.code AS VARCHAR) AS reconciliation_type_code,
        gld.is_reconciled,
        gld.is_reconciled_trial,
        
        -- Entry metadata
        gld.is_generated_entry,
        gld.is_allocation_parent,
        gld.is_allocation_generated,
        gld.allocation_parent_id,
        CAST(gld.entry_group AS VARCHAR) AS entry_group,
        gld.ordinal AS entry_sequence

    FROM source_gl AS gld
    
    -- Core dimensions
    LEFT JOIN companies AS c
        ON gld.company_id = c.id
    LEFT JOIN accounts AS acct
        ON gld.account_id = acct.id
    LEFT JOIN vouchers
        ON gld.voucher_id = vouchers.id
    LEFT JOIN wells
        ON gld.well_id = wells.id
    LEFT JOIN afes
        ON gld.afe_id = afes.id
    
    -- Enriched polymorphic data
    LEFT JOIN location_enriched AS loc
        ON gld.id = loc.gl_id
    LEFT JOIN entity_enriched AS ent
        ON gld.id = ent.gl_id
    
    -- Reference dimensions
    LEFT JOIN source_modules
        ON gld.source_module_code = source_modules.code
    LEFT JOIN payment_types
        ON gld.payment_type_id = payment_types.id
    LEFT JOIN recon_types
        ON gld.reconciliation_type_id = recon_types.id
    
    -- Userfields (search key for wells)
    LEFT JOIN userfields_search_key AS uf
        ON CAST(gld.well_id AS VARCHAR) = uf.well_id
    
    -- Revenue deck chain
    LEFT JOIN rev_deck_revisions
        ON gld.source_revenue_deck_revision_id = rev_deck_revisions.id
    LEFT JOIN rev_decks
        ON rev_deck_revisions.deck_id = rev_decks.id
    
    -- Expense deck chain
    LEFT JOIN exp_deck_revisions
        ON gld.source_expense_deck_revision_id = exp_deck_revisions.id
    LEFT JOIN exp_decks
        ON exp_deck_revisions.deck_id = exp_decks.id
    LEFT JOIN exp_deck_sets
        ON exp_decks.deck_set_id = exp_deck_sets.id
)

SELECT * FROM final