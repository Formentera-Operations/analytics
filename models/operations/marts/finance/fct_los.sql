{{
    config(
        materialized='incremental',
        unique_key='gl_id',
        on_schema_change='sync_all_columns',
        incremental_strategy='merge',
        cluster_by=['company_code', 'journal_date', 'los_category', 'los_section'],
        tags=['marts', 'finance', 'los']
    )
}}

{#
    Mart: Lease Operating Statement Transactions
    
    Purpose: Transaction-level fact table for LOS reporting
    Grain: One row per posted GL transaction on LOS-mapped accounts (gl_id)
    
    Key filters applied:
    - is_posted = true (only posted transactions)
    - is_los_account = true (only accounts in LOS mapping)
    
    Key enrichments from accounts dimension:
    - LOS hierarchy (los_category → los_section → los_line_item_name)
    - Product type (OIL, GAS, NGL, OTHER)
    - Volume/value reporting flags
    - Report line numbers for Power BI sorting
    
    Use cases:
    - Lease Operating Statement by well/property
    - LOE analysis by category
    - Revenue and production tax reporting
    - Workover and P&A tracking
    
    Dependencies:
    - int_gl_enhanced
    - dim_accounts
#}

WITH gl_posted AS (
    SELECT *
    FROM {{ ref('int_gl_enhanced') }}
    WHERE is_posted = TRUE
    {% if is_incremental() %}
        AND _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
    {% endif %}
),

accounts AS (
    SELECT *
    FROM {{ ref('dim_accounts') }}
    WHERE is_los_account = TRUE
),

los_transactions AS (
    SELECT
        -- =================================================================
        -- Keys and Metadata
        -- =================================================================
        gl.gl_id,
        gl._loaded_at,
        gl._last_refresh_at,
        gl.created_at,
        gl.updated_at,
        
        -- =================================================================
        -- Company
        -- =================================================================
        gl.company_code,
        gl.company_name,
        
        -- =================================================================
        -- Account (from GL)
        -- =================================================================
        gl.account_id,
        gl.main_account,
        gl.sub_account,
        gl.account_name,
        
        -- =================================================================
        -- LOS Classification (from Accounts Dimension)
        -- =================================================================
        acct.los_category,
        acct.los_section,
        acct.los_key_sort,
        acct.los_line_item_name,
        acct.los_product_type,
        acct.los_volume_line_number,
        acct.los_value_line_number,
        acct.has_volume_reporting,
        acct.has_value_reporting,
        acct.is_los_subtraction,
        acct.interest_type,
        acct.commodity_type,
        acct.expense_classification,
        
        -- =================================================================
        -- Location (Polymorphic)
        -- =================================================================
        gl.location_type,
        gl.location_code,
        gl.location_name,
        
        -- =================================================================
        -- Entity (Polymorphic - separate IDs for Power BI joins)
        -- =================================================================
        gl.entity_type,
        gl.owner_entity_id,
        gl.vendor_entity_id,
        gl.purchaser_entity_id,
        gl.entity_code,
        gl.entity_name,
        
        -- =================================================================
        -- Well
        -- =================================================================
        gl.well_id,
        gl.well_code,
        gl.well_name,
        gl.op_ref,
        gl.search_key,
        
        -- =================================================================
        -- AFE
        -- =================================================================
        gl.afe_id,
        gl.afe_code,
        gl.afe_type_code,
        gl.afe_type_label,
        
        -- =================================================================
        -- Dates
        -- =================================================================
        gl.journal_date,
        gl.journal_month_start,
        gl.journal_year,
        gl.accrual_date,
        gl.accrual_month_start,
        gl.accrual_year,
        gl.cash_date,
        gl.cash_month_start,
        gl.cash_year,
        
        -- =================================================================
        -- Posting Info
        -- =================================================================
        gl.voucher_id,
        gl.voucher_code,
        gl.posted_at,
        gl.posted_at_cst,
        
        -- =================================================================
        -- Source & Reference
        -- =================================================================
        gl.source_module_code,
        gl.source_module_name,
        gl.payment_type_code,
        gl.reference,
        gl.gl_description,
        
        -- =================================================================
        -- Financial Values
        -- =================================================================
        gl.gross_amount,
        gl.net_amount,
        gl.gross_volume,
        gl.net_volume,
        
        -- Signed values based on LOS subtraction flag
        CASE 
            WHEN acct.is_los_subtraction THEN gl.gross_amount * -1
            ELSE gl.gross_amount
        END AS los_gross_amount,
        
        CASE 
            WHEN acct.is_los_subtraction THEN gl.net_amount * -1
            ELSE gl.net_amount
        END AS los_net_amount,
        
        CASE 
            WHEN acct.is_los_subtraction THEN gl.gross_volume * -1
            ELSE gl.gross_volume
        END AS los_gross_volume,
        
        CASE 
            WHEN acct.is_los_subtraction THEN gl.net_volume * -1
            ELSE gl.net_volume
        END AS los_net_volume,
        
        -- =================================================================
        -- Revenue/Expense Deck
        -- =================================================================
        gl.revenue_deck_revision,
        gl.revenue_deck_effective_date,
        gl.expense_deck_set_code,
        gl.expense_deck_revision,
        gl.expense_deck_effective_date,
        
        -- =================================================================
        -- Entry Metadata
        -- =================================================================
        gl.is_generated_entry,
        gl.is_allocation_parent,
        gl.is_allocation_generated,
        gl.entry_group,
        gl.entry_sequence

    FROM gl_posted gl
    INNER JOIN accounts acct
        ON gl.account_id = acct.account_id
)

SELECT * FROM los_transactions