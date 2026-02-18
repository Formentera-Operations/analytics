{{
    config(
        materialized='table',
        tags=['intermediate', 'finance', 'accounts']
    )
}}

{#
    Intermediate model: Classified Chart of Accounts
    
    Purpose: Enriches accounts with hierarchy and LOS reporting attributes
    Grain: One row per account (account_id)
    
    Key enrichments:
    - Account type/subtype hierarchy from ODA
    - LOS (Lease Operating Statement) classifications from SharePoint
    - Financial statement classification (P&L vs Balance Sheet)
    - Interest type parsing (WI, RI, ORRI, etc.)
    - Reporting capability flags (volume vs value)
    
    Use cases:
    - LOS reporting (posted transactions, specific accounts)
    - Trial balance
    - AP/AR aging
    - General GL analysis
    
    Dependencies:
    - stg_oda__account_v2
    - stg_oda__account_sub_types
    - stg_oda__account_types
    - stg_sharepoint__los_account_map
#}

with accounts_hierarchy as (
    select
        a.id as account_id,
        a.account_code as account_code,
        a.account_name as account_name,
        a.account_full_name as account_full_name,
        a.main_account,
        a.sub_account,
        a.is_active as is_active,
        a.is_normally_debit as is_normally_debit,

        -- Account type hierarchy
        at.type_code,
        at.type_name,
        at.type_full_name,
        ast.subtype_code,
        ast.subtype_name,
        ast.subtype_full_name,
        ast.is_normally_debit as subtype_normally_debit

    from {{ ref('stg_oda__account_v2') }} a
    left join {{ ref('stg_oda__account_sub_types') }} ast
        on a.account_subtype_id = ast.id
    left join {{ ref('stg_oda__account_types') }} at
        on ast.account_type_id = at.id
),

-- Deduplicate LOS mapping to one row per account
-- Accounts can have both volume (NET QTY AMT) and value (NET VALUE AMT) rows
los_mapping as (
    select
        account_code,

        -- Common attributes (consistent across value types for same account)
        max(key_sort_category) as los_key_sort,
        max(line_item_name) as los_line_item_name,
        max(product_type) as los_product_type,
        max(report_header_category) as los_report_header,

        -- Separate line numbers for report sequencing (Power BI sort order)
        max(case when value_type = 'NET QTY AMT' then line_number end) as los_volume_line_number,
        max(case when value_type = 'NET VALUE AMT' then line_number end) as los_value_line_number,

        -- Reporting capability flags
        max(coalesce(value_type = 'NET QTY AMT', false)) as has_volume_reporting,
        max(coalesce(value_type = 'NET VALUE AMT', false)) as has_value_reporting,

        -- Calculation flags for report logic
        max(is_subtraction) as is_los_subtraction,
        max(is_calculated_row) as is_los_calculated

    from {{ ref('stg_sharepoint__los_account_map') }}
    where account_code is not null
    group by account_code
),

classified as (
    select
        -- =================================================================
        -- Account Identity
        -- =================================================================
        ah.account_id,
        ah.account_code,
        ah.account_name,
        ah.account_full_name,
        ah.main_account,
        ah.sub_account,
        ah.is_active,
        ah.is_normally_debit,

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
        lm.los_key_sort,

        lm.los_line_item_name,

        lm.los_product_type,

        -- =================================================================
        -- LOS Attributes (from SharePoint)
        -- =================================================================
        lm.los_report_header,
        lm.los_volume_line_number,
        lm.los_value_line_number,
        lm.has_volume_reporting,
        lm.has_value_reporting,
        lm.is_los_subtraction,
        lm.is_los_calculated,
        lm.los_report_header as los_section,
        case
            when ah.subtype_code = 'R' then 'REVENUE'
            when ah.subtype_code = 'X' then 'EXPENSE'
            when ah.subtype_code = 'A' then 'ASSET'
            when ah.subtype_code = 'L' then 'LIABILITY'
            when ah.subtype_code = 'E' then 'EQUITY'
            else 'OTHER'
        end as financial_statement_type,
        coalesce(ah.subtype_code in ('R', 'X'), false) as is_income_statement_account,

        -- LOS account flag (is this account in the LOS mapping?)
        coalesce(ah.type_code = 'B', false) as is_balance_sheet_account,

        -- =================================================================
        -- LOS Groupings (from SharePoint mapping)
        -- =================================================================

        -- High-level category for broad rollups
        coalesce(lm.account_code is not null, false) as is_los_account,

        -- Detail section (direct from SharePoint report header)
        case
            when lm.los_report_header = 'DEV CAPEX' then 'Capital'
            when
                lm.los_report_header in ('MIDSTREAM CAPEX', 'MIDSTREAM DEAL COSTS', 'MIDSTREAM GL INJECTION')
                then 'Midstream'
            when lm.los_report_header = 'O&G LEASEHOLD' then 'Leasehold'
            when lm.los_report_header in ('OIL REVENUE', 'GAS REVENUE', 'NGL REVENUE') then 'Commodity Revenue'
            when
                lm.los_report_header in ('OIL REVENUE DEDUCTS', 'GAS REVENUE DEDUCTS', 'NGL REVENUE DEDUCTS')
                then 'Revenue Deductions'
            when
                lm.los_report_header in (
                    'OIL PRODUCTION TAXES', 'GAS PRODUCTION TAXES', 'NGL PRODUCTION TAXES', 'AD VAL TAXES'
                )
                then 'Production & Ad Valorem Taxes'
            when lm.los_report_header in ('OVERHEAD INCOME', 'WELL SERVICING INCOME', 'MISC INCOME') then 'Other Income'
            when lm.los_report_header = 'WORKOVER EXPENSES' then 'Workover'
            when lm.los_report_header = 'P&A' then 'Abandonment'
            when lm.los_report_header in ('HEDGE SETTLEMENTS', 'CANCELED HEDGES') then 'Derivatives'
            when lm.los_report_header in (
                'LEASE MAINTENANCE', 'SERVICES & REPAIRS', 'SURFACE EQUIPMENT', 'WELL SERVICING & DH EQUIP',
                'COMPANY LABOR', 'CONTRACT LABOR & SUPERVISION', 'CHEMICALS & TREATING', 'RENTAL EQUIPMENT',
                '3RD PTY WTR & DSPL', 'COMPANY WTR & DISPOSAL', 'FUEL & POWER', 'WEATHER',
                'COPAS OVERHEAD', 'NON-OP LOE', 'DALY WATERS'
            ) then 'Lease Operating Expenses'
            when lm.los_report_header in (
                'CMPNY PR & BNFT', 'CNSL & CNTR EMP', 'HARDWR & SOFTWR', 'OFFICE RENT',
                'CORP FEES', 'CORP INSURANCE', 'AUDIT', 'LEGAL', 'REAL PROP TAX',
                'TRAVEL', 'UTIL & INTERNET', 'VEHICLES', 'SUPPLIES & EQP', 'MISCELLANEOUS'
            ) then 'G&A'
            when lm.los_report_header = 'INVENTORY' then 'Inventory'
            when lm.los_report_header in ('OTHER', 'ACCRUAL') then 'Other'
        end as los_category,

        -- =================================================================
        -- Interest Type (parsed from account name/code)
        -- =================================================================
        case
            when
                ah.account_name ilike '%WI%'
                or ah.account_name ilike '%working%'
                or ah.sub_account = '1' then 'Working Interest'
            when
                ah.account_name ilike '%RI%'
                or ah.account_name ilike '%royalty%'
                or ah.sub_account = '2' then 'Royalty Interest'
            when
                ah.account_name ilike '%ORRI%'
                or ah.account_name ilike '%overriding%'
                or ah.sub_account = '3' then 'Overriding Royalty'
            when
                ah.account_name ilike '%hedge%'
                or ah.sub_account like '2_%' then 'Hedging'
            when
                ah.account_name ilike '%accrued%'
                or ah.sub_account = '5' then 'Accruals'
            when
                ah.account_name ilike '%deduct%'
                or ah.main_account in ('84', '85', '86', '87') then 'Deductions'
            else 'Base'
        end as interest_type,

        -- =================================================================
        -- Commodity Type (prefer LOS mapping, fallback to account code)
        -- =================================================================
        case
            when lm.los_product_type is not null then lm.los_product_type
            when ah.main_account = '701' then 'OIL'
            when ah.main_account = '702' then 'GAS'
            when ah.main_account = '703' then 'NGL'
            else 'OTHER'
        end as commodity_type,

        -- =================================================================
        -- Expense Classification
        -- =================================================================
        case
            when lm.los_key_sort = 'WORKOVER' then 'WORKOVER'
            when lm.los_key_sort = 'P&A' then 'ABANDONMENT'
            when lm.los_key_sort = 'HEDGES' then 'DERIVATIVE'
            when lm.los_key_sort is not null and ah.subtype_code = 'R' then 'REVENUE'
            when lm.los_key_sort is not null and ah.subtype_code = 'X' then 'LOE'
            when ah.main_account in ('310', '328', '301') then 'CAPITAL'
            when ah.subtype_code = 'R' then 'REVENUE'
            when ah.subtype_code = 'X' then 'OPERATING'
            else 'OTHER'
        end as expense_classification

    from accounts_hierarchy ah
    left join los_mapping lm
        on ah.account_code = lm.account_code
)

select * from classified
