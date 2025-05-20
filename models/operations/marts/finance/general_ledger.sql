{{
    config(
        materialized='incremental',
        unique_key='gl_id',
        incremental_strategy='merge',
        cluster_by=['company_code', 'journal_date', 'account_key'],
        tags=['finance', 'gl', 'mart']
    )
}}

WITH gl_enriched AS (
    SELECT *
    FROM {{ ref('int_general_ledger_enhanced') }}
),

-- Reference your existing calendar dimension
date_dim AS (
    SELECT *
    FROM {{ ref('stg_oda__calendar') }}
),

-- Account categorization for financial reporting
account_aggregation AS (
    SELECT
        main_account || '-' || sub_account AS account_key,
        CASE
            WHEN main_account BETWEEN 100 AND 199 THEN 'Assets'
            WHEN main_account BETWEEN 200 AND 299 THEN 'Liabilities'
            WHEN main_account BETWEEN 300 AND 399 THEN 'Equity'
            WHEN main_account BETWEEN 400 AND 499 THEN 'Revenue'
            WHEN main_account BETWEEN 500 AND 699 THEN 'Expenses'
            WHEN main_account BETWEEN 700 AND 799 THEN 'Production'
            WHEN main_account BETWEEN 800 AND 899 THEN 'Other Income/Expense'
            ELSE 'Other'
        END AS account_category,
        
        CASE 
            WHEN main_account IN (701, 702, 703) AND sub_account IN (1, 2, 3, 4, 5) THEN 'Volume'
            ELSE 'Value'
        END AS transaction_type
    FROM gl_enriched
    GROUP BY 1, 2, 3
)

SELECT
    -- Unique identifiers and metadata
    gl.gl_id,
    gl.created_date,
    gl.updated_date,
    gl.last_refresh_time,
    
    -- Company and period dimensions
    gl.company_code,
    gl.company_name AS company_name,  -- Assuming this exists in int model
    
    -- Date dimensions with enhanced calendar attributes
    gl.journal_date,
    gl.journal_month,
    gl.journal_year,
    gl.journal_month_year,
    
    -- Calendar dimension enhancements
    cal_j.quarter AS fiscal_quarter,
    cal_j.quarter_name AS fiscal_quarter_name,
    cal_j.month AS fiscal_month,
    cal_j.month_name AS fiscal_month_name,
    cal_j.week_of_year AS fiscal_week,
    cal_j.is_weekend AS is_weekend,
    cal_j.first_date_of_month AS first_date_of_month,
    cal_j.last_date_of_month AS last_date_of_month,
    
    -- Account dimensions with hierarchy
    gl.main_account,
    gl.sub_account,
    gl.account_name,
    gl.main_account || '-' || gl.sub_account AS account_key,
    agg.account_category,
    agg.transaction_type,
    
    -- Source information
    gl.source_module_code,
    gl.source_module_name,
    gl.voucher_code,
    
    -- Document and reference information
    gl.posted,
    gl.posted_date,
    gl.posted_date_time_cst,
    gl.reference_type,
    gl.reference,
    
    -- Entity information (who/what this transaction relates to)
    gl.entity,
    gl.entity_name,
    
    -- Location information (where this transaction is allocated)
    gl.location_type,
    gl.location_code,
    gl.location_name,
    
    -- Well information
    gl.well_code,
    gl.well_name,
    
    -- AFE information for capital tracking
    gl.afe_code,
    
    -- Transaction description
    gl.gl_description,
    
    -- Financial values
    gl.gross_amount,
    gl.net_amount,
    gl.gross_volume,
    gl.net_volume,
    
    -- Add calculated columns
    CASE 
        WHEN gl.gross_amount != 0 AND gl.gross_volume != 0 
        THEN gl.gross_amount / NULLIF(gl.gross_volume, 0)
        ELSE NULL
    END AS gross_price_per_unit,
    
    CASE 
        WHEN gl.net_amount != 0 AND gl.net_volume != 0 
        THEN gl.net_amount / NULLIF(gl.net_volume, 0)
        ELSE NULL
    END AS net_price_per_unit,
    
    -- Budget/Actual comparison fields
    gl.revenue_deck_change_code,
    gl.revenue_deck_version_date,
    gl.expense_deck,
    gl.expense_deck_change_code,
    gl.expense_deck_version_date,
    
    -- Reconciliation status
    gl.reconciliation_type,
    gl.reconciled,
    gl.reconciled_trial,
    
    -- Reporting flags
    gl.include_in_journal_report,
    gl.include_in_cash_report,
    gl.include_in_accrual_report,
    gl.present_in_journal_balance,
    gl.present_in_cash_balance,
    gl.present_in_accrual_balance,
    
    -- Entry metadata
    gl.generated_entry,
    gl.entry_group,
    gl.entry_seq,
    
    -- Additional date dimensions with calendar enhancements
    gl.accrual_date,
    gl.accrual_month,
    gl.accrual_year,
    gl.accrual_month_year,
    cal_a.quarter AS accrual_fiscal_quarter,
    cal_a.month_name AS accrual_month_name,
    
    gl.cash_date,
    gl.cash_month,
    gl.cash_year,
    gl.cash_month_year,
    cal_c.quarter AS cash_fiscal_quarter,
    cal_c.month_name AS cash_month_name

FROM gl_enriched gl
LEFT JOIN date_dim cal_j
    ON TO_DATE(gl.journal_date) = cal_j.date
LEFT JOIN date_dim cal_a
    ON TO_DATE(gl.accrual_date) = cal_a.date
LEFT JOIN date_dim cal_c
    ON TO_DATE(gl.cash_date) = cal_c.date
LEFT JOIN account_aggregation agg
    ON gl.main_account || '-' || gl.sub_account = agg.account_key

WHERE 1=1

{% if is_incremental() %}
    -- Only process new or updated GL entries since last run
    AND (
        gl.created_date > (SELECT MAX(created_date) FROM {{ this }})
        OR gl.updated_date > (SELECT MAX(updated_date) FROM {{ this }})
    )
{% endif %}