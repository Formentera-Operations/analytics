{{
    config(
        materialized='incremental',
        unique_key='gl_id',
        on_schema_change='sync_all_columns',
        incremental_strategy='merge',
        cluster_by=['company_code', 'journal_date', 'main_account', 'sub_account']
    )
}}

WITH source_gl AS (
    SELECT *
    FROM {{ ref('stg_oda__gl') }}
),

vouchers AS (
    SELECT *
    FROM {{ ref('stg_oda__voucher_v2') }}
),

accounts AS (
    SELECT *
    FROM {{ ref('stg_oda__account_v2') }}
),

companies AS (
    SELECT *
    FROM {{ ref('stg_oda__company_v2') }}
),

entities AS (
    SELECT *
    FROM {{ ref('stg_oda__entity_v2') }}
),

owners AS (
    SELECT *
    FROM {{ ref('stg_oda__owner_v2') }}
),

purchasers AS (
    SELECT *
    FROM {{ ref('stg_oda__purchaser_v2') }}
),

vendors AS (
    SELECT *
    FROM {{ ref('stg_oda__vendor_v2') }}
),

wells AS (
    SELECT *
    FROM {{ ref('stg_oda__wells') }}
),

userfields AS (
    SELECT 
        "Id",
        "UserFieldName",
        "UserFieldValueString"
    FROM {{ ref('stg_oda__userfield') }}
    WHERE "UserFieldName" = 'UF-SEARCH KEY'
GROUP BY ALL
),

source_modules AS (
    SELECT *
    FROM {{ ref('stg_oda__source_module') }}
),

rev_deck_revisions AS (
    SELECT *
    FROM {{ ref('stg_oda__revenue_deck_revision') }}
),

rev_decks AS (
    SELECT *
    FROM {{ ref('stg_oda__revenue_deck_v2') }}
),

rev_deck_sets AS (
    SELECT *
    FROM {{ ref('stg_oda__revenue_deck_set') }}
),

payment_types AS (
    SELECT *
    FROM {{ ref('stg_oda__payment_type') }}
),

afes AS (
    SELECT *
    FROM {{ ref('stg_oda__afe_v2') }}
),

exp_deck_revisions AS (
    SELECT *
    FROM {{ ref('stg_oda__expense_deck_revision') }}
),

exp_decks AS (
    SELECT *
    FROM {{ ref('stg_oda__expense_deck_v2') }}
),

exp_deck_sets AS (
    SELECT *
    FROM {{ ref('stg_oda__expense_deck_set') }}
),

recon_types AS (
    SELECT *
    FROM {{ ref('stg_oda__gl_reconciliation_type') }}
),

location_codes AS (
    -- Pre-process all location codes to ensure consistent type
    SELECT
        gld.id AS gl_id,
        CAST(loc_company.code AS VARCHAR) AS company_code,
        CAST(loc_owner_entity.code AS VARCHAR) AS owner_entity_code,
        CAST(loc_purchaser_entity.code AS VARCHAR) AS purchaser_entity_code,
        CAST(loc_vendor_entity.code AS VARCHAR) AS vendor_entity_code,
        CAST(loc_well.code AS VARCHAR) AS well_code
    FROM source_gl AS gld
    LEFT OUTER JOIN companies AS loc_company
        ON gld.location_company_id = loc_company.id
    LEFT OUTER JOIN owners AS loc_owner
        ON gld.location_owner_id = loc_owner.id
    LEFT OUTER JOIN entities AS loc_owner_entity
        ON loc_owner.entity_id = loc_owner_entity.id
    LEFT OUTER JOIN purchasers AS loc_purchaser
        ON gld.location_purchaser_id = loc_purchaser.id
    LEFT OUTER JOIN entities AS loc_purchaser_entity
        ON loc_purchaser.entity_id = loc_purchaser_entity.id
    LEFT OUTER JOIN vendors AS loc_vendor
        ON gld.location_vendor_id = loc_vendor.id
    LEFT OUTER JOIN entities AS loc_vendor_entity
        ON loc_vendor.entity_id = loc_vendor_entity.id
    LEFT OUTER JOIN wells AS loc_well
        ON gld.location_well_id = loc_well.id
),

location_names AS (
    -- Pre-process all location names to ensure consistent type
    SELECT
        gld.id AS gl_id,
        CAST(loc_company.name AS VARCHAR) AS company_name,
        CAST(loc_owner_entity.name AS VARCHAR) AS owner_entity_name,
        CAST(loc_purchaser_entity.name AS VARCHAR) AS purchaser_entity_name,
        CAST(loc_vendor_entity.name AS VARCHAR) AS vendor_entity_name,
        CAST(loc_well.name AS VARCHAR) AS well_name
    FROM source_gl AS gld
    LEFT OUTER JOIN companies AS loc_company
        ON gld.location_company_id = loc_company.id
    LEFT OUTER JOIN owners AS loc_owner
        ON gld.location_owner_id = loc_owner.id
    LEFT OUTER JOIN entities AS loc_owner_entity
        ON loc_owner.entity_id = loc_owner_entity.id
    LEFT OUTER JOIN purchasers AS loc_purchaser
        ON gld.location_purchaser_id = loc_purchaser.id
    LEFT OUTER JOIN entities AS loc_purchaser_entity
        ON loc_purchaser.entity_id = loc_purchaser_entity.id
    LEFT OUTER JOIN vendors AS loc_vendor
        ON gld.location_vendor_id = loc_vendor.id
    LEFT OUTER JOIN entities AS loc_vendor_entity
        ON loc_vendor.entity_id = loc_vendor_entity.id
    LEFT OUTER JOIN wells AS loc_well
        ON gld.location_well_id = loc_well.id
),

entity_codes AS (
    -- Pre-process all entity codes to ensure consistent type
    SELECT
        gld.id AS gl_id,
        CAST(ent_company.code AS VARCHAR) AS company_code,
        CAST(ent_owner_entity.code AS VARCHAR) AS owner_entity_code,
        CAST(ent_purchaser_entity.code AS VARCHAR) AS purchaser_entity_code,
        CAST(ent_vendor_entity.code AS VARCHAR) AS vendor_entity_code
    FROM source_gl AS gld
    LEFT OUTER JOIN companies AS ent_company
        ON gld.entity_company_id = ent_company.id
    LEFT OUTER JOIN owners AS ent_owner
        ON gld.entity_owner_id = ent_owner.id
    LEFT OUTER JOIN entities AS ent_owner_entity
        ON ent_owner.entity_id = ent_owner_entity.id
    LEFT OUTER JOIN purchasers AS ent_purchaser
        ON gld.entity_purchaser_id = ent_purchaser.id
    LEFT OUTER JOIN entities AS ent_purchaser_entity
        ON ent_purchaser.entity_id = ent_purchaser_entity.id
    LEFT OUTER JOIN vendors AS ent_vendor
        ON gld.entity_vendor_id = ent_vendor.id
    LEFT OUTER JOIN entities AS ent_vendor_entity
        ON ent_vendor.entity_id = ent_vendor_entity.id
),

entity_names AS (
    -- Pre-process all entity names to ensure consistent type
    SELECT
        gld.id AS gl_id,
        CAST(ent_company.name AS VARCHAR) AS company_name,
        CAST(ent_owner_entity.name AS VARCHAR) AS owner_entity_name,
        CAST(ent_purchaser_entity.name AS VARCHAR) AS purchaser_entity_name,
        CAST(ent_vendor_entity.name AS VARCHAR) AS vendor_entity_name
    FROM source_gl AS gld
    LEFT OUTER JOIN companies AS ent_company
        ON gld.entity_company_id = ent_company.id
    LEFT OUTER JOIN owners AS ent_owner
        ON gld.entity_owner_id = ent_owner.id
    LEFT OUTER JOIN entities AS ent_owner_entity
        ON ent_owner.entity_id = ent_owner_entity.id
    LEFT OUTER JOIN purchasers AS ent_purchaser
        ON gld.entity_purchaser_id = ent_purchaser.id
    LEFT OUTER JOIN entities AS ent_purchaser_entity
        ON ent_purchaser.entity_id = ent_purchaser_entity.id
    LEFT OUTER JOIN vendors AS ent_vendor
        ON gld.entity_vendor_id = ent_vendor.id
    LEFT OUTER JOIN entities AS ent_vendor_entity
        ON ent_vendor.entity_id = ent_vendor_entity.id
)


SELECT 
    -- Metadata and timestamps
    CONVERT_TIMEZONE('UTC', 'America/Chicago', CURRENT_TIMESTAMP())::TIMESTAMP_TZ AS last_refresh_time,
    gld.id AS gl_id,
    gld.created_at::TIMESTAMP_NTZ AS created_date,
    gld.updated_at::TIMESTAMP_NTZ AS updated_date,
    
    -- Company and account info
    c.code AS company_code,
    c.name as company_name,
    acct.main_account AS main_account,
    acct.sub_account AS sub_account,
    acct.account_name AS account_name,

    -- AFE Type Classification
    CAST(afes.afe_type_id AS VARCHAR) AS afe_type_id,
    CAST(afes.afe_type_code AS VARCHAR) AS afe_type_code,
    CAST(afes.afe_type_label AS VARCHAR) AS afe_type_label,
    CAST(afes.afe_type_full_name AS VARCHAR) AS afe_type_full_name,
    
    -- Location information with CASE logic
    CASE
        WHEN lc.company_code IS NOT NULL THEN 'Company'
        WHEN lc.owner_entity_code IS NOT NULL THEN 'Owner'
        WHEN lc.purchaser_entity_code IS NOT NULL THEN 'Purchaser'
        WHEN lc.vendor_entity_code IS NOT NULL THEN 'Vendor'
        WHEN lc.well_code IS NOT NULL THEN 'Well'
    END::VARCHAR AS location_type,
    
    -- Use the pre-processed location code and name
    COALESCE(
        lc.company_code,
        lc.owner_entity_code,
        lc.purchaser_entity_code,
        lc.vendor_entity_code,
        lc.well_code
    )::VARCHAR AS location_code,
    
    COALESCE(
        ln.company_name,
        ln.owner_entity_name,
        ln.purchaser_entity_name,
        ln.vendor_entity_name,
        ln.well_name
    )::VARCHAR AS location_name,
    
    -- Posted information
    CASE WHEN gld.is_posted THEN 'Y' ELSE 'N' END::VARCHAR AS posted,
    vouchers.posted_date::TIMESTAMP_NTZ AS posted_date_time,
    CONVERT_TIMEZONE('UTC', 'America/Chicago', vouchers.posted_date)::TIMESTAMP_TZ AS posted_date_time_cst,
    TO_CHAR(vouchers.posted_date, 'MM-DD-YYYY')::VARCHAR AS posted_date,
    
    -- Date dimensions - Accrual
    gld.accrual_date::DATE AS accrual_date,
    TO_CHAR(gld.accrual_date, 'MM')::VARCHAR AS accrual_month,
    TO_CHAR(gld.accrual_date, 'YYYY')::VARCHAR AS accrual_year,
    TO_CHAR(gld.accrual_date, 'MM-YYYY')::VARCHAR AS accrual_month_year,

    -- Date dimensions - Cash
    gld.cash_date::DATE AS cash_date,
    TO_CHAR(gld.cash_date, 'MM')::VARCHAR AS cash_month,
    TO_CHAR(gld.cash_date, 'YYYY')::VARCHAR AS cash_year,
    TO_CHAR(gld.cash_date, 'MM-YYYY')::VARCHAR AS cash_month_year,

    -- Date dimensions - Journal
    gld.journal_date::DATE AS journal_date,
    TO_CHAR(gld.journal_date, 'MM')::VARCHAR AS journal_month,
    TO_CHAR(gld.journal_date, 'YYYY')::VARCHAR AS journal_year,
    TO_CHAR(gld.journal_date, 'MM-YYYY')::VARCHAR AS journal_month_year,

    -- Source and reference information
    CAST(gld.source_module_code AS VARCHAR) AS source_module_code,
    CAST(source_modules.name AS VARCHAR) AS source_module_name,
    CAST(vouchers.code AS VARCHAR) AS voucher_code,
    CAST(vouchers.voucher_type_id AS VARCHAR) AS voucher_type_id,
    CAST(wells.property_reference_code AS VARCHAR) AS op_ref,
    CAST(wells.code AS VARCHAR) AS well_code,
    wells.id as well_id,
    CAST(wells.name AS VARCHAR) AS well_name,
    CAST(loc_search."UserFieldValueString" AS VARCHAR) AS search_key,
    
    -- Report inclusion flags
    gld.is_include_in_journal_report AS include_in_journal_report,
    gld.is_present_in_journal_balance AS present_in_journal_balance,
    gld.is_include_in_cash_report AS include_in_cash_report,
    gld.is_present_in_cash_balance AS present_in_cash_balance,
    gld.is_include_in_accrual_report AS include_in_accrual_report,
    gld.is_present_in_accrual_balance AS present_in_accrual_balance,
    
    -- Revenue deck information
    CAST(rev_deck_revisions.revision_number AS VARCHAR) AS revenue_deck_change_code,
    rev_decks.effective_date::DATE AS revenue_deck_version_date,
    CAST(rev_deck_revisions.nri_actual AS VARCHAR) AS net_revenue_interest_actual,
    CAST(rev_deck_revisions.total_interest_expected AS VARCHAR) AS total_interest_expected,
    
    -- Reference and currency
    CAST(payment_types.code AS VARCHAR) AS reference_type,
    CAST(gld.reference AS VARCHAR) AS reference,
    CAST(c.code AS VARCHAR) AS currency_code,
    CAST(afes.code AS VARCHAR) AS afe_code,
    
    -- Entity information using pre-processed entity data
    CAST(COALESCE(
        'C: ' || ec.company_code,
        'O: ' || ec.owner_entity_code,
        'P: ' || ec.purchaser_entity_code,
        'V: ' || ec.vendor_entity_code                
    ) AS VARCHAR) AS entity,
    
    CAST(COALESCE(
        en.company_name,
        en.owner_entity_name,
        en.purchaser_entity_name,
        en.vendor_entity_name                
    ) AS VARCHAR) AS entity_name,
    
    -- Transaction values
    CAST(gld.description AS VARCHAR) AS gl_description,
    gld.gross_value::FLOAT AS gross_amount,
    gld.net_value::FLOAT AS net_amount,
    gld.gross_volume::FLOAT AS gross_volume,
    gld.net_volume::FLOAT AS net_volume,
    
    -- Expense deck information
    CAST(exp_deck_sets.code AS VARCHAR) AS expense_deck,
    CAST(exp_deck_revisions.revision_number AS VARCHAR) AS expense_deck_change_code,
    exp_decks.effective_date::DATE AS expense_deck_version_date,
    exp_deck_revisions.total_interest_actual::FLOAT AS expense_deck_interest_total,
    
    -- Reconciliation information
    CAST(recon_types.code AS VARCHAR) AS reconciliation_type,
    gld.is_reconciled_trial AS reconciled_trial,
    gld.is_reconciled AS reconciled,
    
    -- Entry metadata
    gld.is_generated_entry AS generated_entry,
    CAST(gld.entry_group AS VARCHAR) AS entry_group,
    gld.ordinal AS entry_seq

FROM source_gl AS gld
LEFT OUTER JOIN companies AS c
    ON c.id = gld.company_id
LEFT OUTER JOIN accounts AS acct
    ON gld.account_id = acct.account_id
    
-- Use the pre-processed location data
LEFT OUTER JOIN location_codes lc
    ON gld.id = lc.gl_id
LEFT OUTER JOIN location_names ln
    ON gld.id = ln.gl_id
    
-- Use the pre-processed entity data
LEFT OUTER JOIN entity_codes ec
    ON gld.id = ec.gl_id
LEFT OUTER JOIN entity_names en
    ON gld.id = en.gl_id
    
-- Other joins
LEFT OUTER JOIN source_modules
    ON gld.source_module_code = source_modules.code
LEFT OUTER JOIN vouchers
    ON gld.voucher_id = vouchers.id
LEFT OUTER JOIN wells
    ON gld.well_id = wells.id
LEFT OUTER JOIN userfields AS loc_search
        ON gld.location_well_id = loc_search."Id"
LEFT OUTER JOIN rev_deck_revisions
    ON gld.source_revenue_deck_revision_id = rev_deck_revisions.id
LEFT OUTER JOIN rev_decks
    ON rev_deck_revisions.deck_id = rev_decks.id
LEFT OUTER JOIN rev_deck_sets
    ON rev_decks.deck_set_id = rev_deck_sets.id
LEFT OUTER JOIN payment_types
    ON gld.payment_type_id = payment_types.id
LEFT OUTER JOIN afes
    ON gld.afe_id = afes.id
LEFT OUTER JOIN exp_deck_revisions
    ON gld.source_expense_deck_revision_id = exp_deck_revisions.id
LEFT OUTER JOIN exp_decks
    ON exp_deck_revisions.deck_id = exp_decks.id
LEFT OUTER JOIN exp_deck_sets
    ON exp_decks.deck_set_id = exp_deck_sets.id
LEFT OUTER JOIN recon_types
    ON gld.reconciliation_type_id = recon_types.id

WHERE 1=1
{% if is_incremental() %}
    -- Only process new or updated GL entries since last run
    AND (
        gld.created_at > (SELECT MAX(created_date) FROM {{ this }})
        OR gld.updated_at > (SELECT MAX(updated_date) FROM {{ this }})
    )
{% endif %}