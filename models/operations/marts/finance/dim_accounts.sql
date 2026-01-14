{{
    config(
        materialized='table',
        tags=['marts', 'finance', 'dimension']
    )
}}

{#
    Dimension: Accounts
    
    Purpose: Enriched chart of accounts with LOS reporting attributes
    Grain: One row per account (account_id)
    
    Key enrichments:
    - Account type/subtype hierarchy from ODA
    - LOS (Lease Operating Statement) classifications from SharePoint
    - Financial statement classification (P&L vs Balance Sheet)
    - Interest type parsing (WI, RI, ORRI, etc.)
    - Reporting capability flags (volume vs value)
    
    Use cases:
    - LOS reporting (joined to fct_los)
    - Trial balance
    - AP/AR aging
    - General GL analysis
    
    Sources:
    - stg_oda__account_v2
    - stg_oda__account_sub_types
    - stg_oda__account_types
    - stg_sharepoint__los_account_map
#}

WITH accounts_hierarchy AS (
    SELECT 
        a.account_id AS account_id,
        a.account_code AS account_code,
        a.account_name AS account_name,
        a.account_full_name AS account_full_name,
        a.main_account,
        a.sub_account,
        case
            when a.sub_account = '' then a.main_account
            else concat(a.main_account, '-', a.sub_account)
        end as combined_account,
        a.is_active AS is_active,
        a.is_normally_debit AS is_normally_debit,
        a.is_accrual as is_accrual,
        
        -- Account type hierarchy
        at.type_code,
        at.type_name,
        at.type_full_name,
        ast.subtype_code,
        ast.subtype_name,
        ast.subtype_full_name,
        ast.normally_debit AS subtype_normally_debit
        
    FROM {{ ref('stg_oda__account_v2') }} a
    LEFT JOIN {{ ref('stg_oda__account_sub_types') }} ast 
        ON a.account_subtype_id = ast.account_subtype_id
    LEFT JOIN {{ ref('stg_oda__account_types') }} at 
        ON ast.account_type_id = at.account_type_id
),

-- Deduplicate LOS mapping to one row per account
-- Accounts can have both volume (NET QTY AMT) and value (NET VALUE AMT) rows
los_mapping AS (
    SELECT
        account_code,
        
        -- Common attributes (consistent across value types for same account)
        MAX(key_sort_category) AS los_key_sort,
        MAX(line_item_name) AS los_line_item_name,
        MAX(product_type) AS los_product_type,
        MAX(report_header_category) AS los_report_header,
        CASE WHEN min(value_type) = 'NET QTY AMT' THEN min(report_header_category) END AS los_volume_report_header,
        
        -- Separate line numbers for report sequencing (Power BI sort order)
        CASE WHEN min(value_type) = 'NET QTY AMT' THEN min(los_mapping_id) END AS los_volume_line_number,
        CASE WHEN max(value_type) = 'NET VALUE AMT' THEN max(los_mapping_id) END AS los_value_line_number,
        max(los_mapping_id) as los_line_number,
        max(line_header_line_number) as los_report_header_line_number,
        
        -- Reporting capability flags
        MAX(CASE WHEN value_type = 'NET QTY AMT' THEN TRUE ELSE FALSE END) AS has_volume_reporting,
        MAX(CASE WHEN value_type = 'NET VALUE AMT' THEN TRUE ELSE FALSE END) AS has_value_reporting,
        
        -- Calculation flags for report logic
        MAX(is_subtraction) AS is_los_subtraction,
        MAX(is_calculated_row) AS is_los_calculated
        
    FROM {{ ref('stg_sharepoint__los_account_map') }}
    WHERE account_code IS NOT NULL
    GROUP BY account_code
),

final AS (
    SELECT
        -- =================================================================
        -- Account Identity
        -- =================================================================
        ah.account_id,
        ah.account_code,
        ah.account_name,
        ah.account_full_name,
        ah.main_account,
        ah.sub_account,
        ah.combined_account,
        ah.is_active,
        ah.is_normally_debit,
        ah.is_accrual,
        
        -- =================================================================
        -- Account Type Hierarchy (from ODA)
        -- =================================================================
        ah.type_code,
        ah.type_name,
        ah.subtype_code,
        ah.subtype_name,
        
        -- =================================================================
        -- Financial Statement Classification
        -- =================================================================
        CASE 
            WHEN ah.subtype_code = 'R' THEN 'REVENUE'
            WHEN ah.subtype_code = 'X' THEN 'EXPENSE'
            WHEN ah.subtype_code = 'A' THEN 'ASSET'
            WHEN ah.subtype_code = 'L' THEN 'LIABILITY'
            WHEN ah.subtype_code = 'E' THEN 'EQUITY'
            ELSE 'OTHER'
        END AS financial_statement_type,
        
        CASE 
            WHEN ah.subtype_code IN ('R', 'X') THEN TRUE
            ELSE FALSE
        END AS is_income_statement_account,
        
        CASE 
            WHEN ah.type_code = 'B' THEN TRUE
            ELSE FALSE
        END AS is_balance_sheet_account,
        
        -- =================================================================
        -- LOS Attributes (from SharePoint)
        -- =================================================================
        lm.los_key_sort,
        lm.los_line_item_name,
        lm.los_product_type,
        lm.los_report_header,
        lm.los_volume_report_header,
        lm.los_volume_line_number,
        lm.los_value_line_number,
        lm.los_report_header_line_number,
        lm.los_line_number,
        lm.has_volume_reporting,
        lm.has_value_reporting,
        lm.is_los_subtraction,
        lm.is_los_calculated,
        
        -- LOS account flag (is this account in the LOS mapping?)
        CASE WHEN lm.account_code IS NOT NULL THEN TRUE ELSE FALSE END AS is_los_account,
        
        -- =================================================================
        -- LOS Groupings (from SharePoint mapping)
        -- =================================================================
        
        -- High-level category for broad rollups
        CASE 
            WHEN lm.los_report_header IN ('DEV CAPEX','MIDSTREAM CAPEX') THEN 'Capital'
            WHEN lm.los_report_header IN ('MIDSTREAM DEAL COSTS') THEN 'Midstream'
            WHEN lm.los_report_header = 'O&G LEASEHOLD' THEN 'Leasehold'
            WHEN lm.los_report_header IN ('OIL REVENUE', 'GAS REVENUE', 'NGL REVENUE') THEN 'Commodity Revenue'
            WHEN lm.los_report_header IN ('OIL REVENUE DEDUCTS', 'GAS REVENUE DEDUCTS', 'NGL REVENUE DEDUCTS') THEN 'Revenue Deductions'
            WHEN lm.los_report_header IN ('OIL PRODUCTION TAXES', 'GAS PRODUCTION TAXES', 'NGL PRODUCTION TAXES', 'AD VAL TAXES') THEN 'Production & Ad Valorem Taxes'
            WHEN lm.los_report_header IN ('OVERHEAD INCOME', 'WELL SERVICING INCOME', 'MISC INCOME') THEN 'Other Income'
            WHEN lm.los_report_header = 'WORKOVER EXPENSES' THEN 'Workover'
            WHEN lm.los_report_header = 'P&A' THEN 'Abandonment'
            WHEN lm.los_report_header IN ('HEDGE SETTLEMENTS', 'CANCELED HEDGES') THEN 'Derivatives'
            WHEN lm.los_report_header IN (
                'LEASE MAINTENANCE', 'SERVICES & REPAIRS', 'SURFACE EQUIPMENT', 'WELL SERVICING & DH EQUIP',
                'COMPANY LABOR', 'CONTRACT LABOR & SUPERVISION', 'CHEMICALS & TREATING', 'RENTAL EQUIPMENT',
                '3RD PTY WTR & DSPL', 'COMPANY WTR & DISPOSAL', 'FUEL & POWER', 'WEATHER', 
                'COPAS OVERHEAD', 'NON-OP LOE', 'DALY WATERS', 'MIDSTREAM GL INJECTION', 'OTHER'
            ) THEN 'Lease Operating Expenses'
            WHEN lm.los_report_header IN (
                'CMPNY PR & BNFT', 'CNSL & CNTR EMP', 'HARDWR & SOFTWR', 'OFFICE RENT', 
                'CORP FEES', 'CORP INSURANCE', 'AUDIT', 'LEGAL', 'REAL PROP TAX', 
                'TRAVEL', 'UTIL & INTERNET', 'VEHICLES', 'SUPPLIES & EQP', 'MISCELLANEOUS'
            ) THEN 'G&A'
            WHEN lm.los_report_header = 'INVENTORY' THEN 'Inventory'
            WHEN lm.los_report_header = 'ACCRUAL' THEN 'Accrual'
            ELSE lm.los_report_header
        END AS los_category,
        
        -- Detail section (direct from SharePoint report header)
        CASE
            WHEN account_full_name ='900 / 305: FIELD OFFICE EXPENSE' THEN 'COMPANY LABOR'
            ELSE lm.los_report_header
        END AS los_section,
        
        -- =================================================================
        -- Interest Type (parsed from account name/code)
        -- =================================================================
        CASE 
            WHEN ah.account_name ILIKE '%WI%' 
                OR ah.account_name ILIKE '%working%' 
                OR ah.sub_account = '1' THEN 'Working Interest'
            WHEN ah.account_name ILIKE '%RI%' 
                OR ah.account_name ILIKE '%royalty%' 
                OR ah.sub_account = '2' THEN 'Royalty Interest'
            WHEN ah.account_name ILIKE '%ORRI%' 
                OR ah.account_name ILIKE '%overriding%' 
                OR ah.sub_account = '3' THEN 'Overriding Royalty'
            WHEN ah.account_name ILIKE '%hedge%' 
                OR ah.sub_account LIKE '2_%' THEN 'Hedging'
            WHEN ah.account_name ILIKE '%accrued%' 
                OR ah.sub_account = '5' THEN 'Accruals'
            WHEN ah.account_name ILIKE '%deduct%' 
                OR ah.main_account IN ('84', '85', '86', '87') THEN 'Deductions'
            ELSE 'Base'
        END AS interest_type,
        
        -- =================================================================
        -- Commodity Type (prefer LOS mapping, fallback to account code)
        -- =================================================================
        CASE 
            WHEN lm.los_product_type IS NOT NULL THEN lm.los_product_type
            WHEN ah.main_account = '701' THEN 'OIL'
            WHEN ah.main_account = '702' THEN 'GAS'
            WHEN ah.main_account = '703' THEN 'NGL'
            ELSE 'OTHER'
        END AS commodity_type,
        
        -- =================================================================
        -- Expense Classification
        -- =================================================================
        CASE 
            WHEN lm.los_key_sort in ('WORKOVER', 'CAP PROD/WKOVR') THEN 'WORKOVER'
            WHEN lm.los_key_sort = 'P&A' THEN 'ABANDONMENT'
            WHEN lm.los_key_sort = 'HEDGES' THEN 'DERIVATIVE'
            WHEN lm.los_key_sort IS NOT NULL AND ah.subtype_code = 'R' THEN 'REVENUE'
            WHEN lm.los_key_sort IS NOT NULL AND ah.subtype_code = 'X' THEN 'LOE'
            WHEN ah.main_account IN ('310', '328', '301') THEN 'CAPITAL'
            WHEN ah.subtype_code = 'R' THEN 'REVENUE'
            WHEN ah.subtype_code = 'X' THEN 'OPERATING'
            ELSE 'OTHER'
        END AS expense_classification,
        
        -- =================================================================
        -- Metadata
        -- =================================================================
        CURRENT_TIMESTAMP() AS _refreshed_at

    FROM accounts_hierarchy ah
    LEFT JOIN los_mapping lm 
        ON ah.account_code = lm.account_code
)

SELECT * FROM final order by los_line_number