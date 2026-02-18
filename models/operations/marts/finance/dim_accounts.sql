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
        a.is_accrual as is_accrual,
        at.type_code,

        -- Account type hierarchy
        at.type_name,
        at.type_full_name,
        ast.subtype_code,
        ast.subtype_name,
        ast.subtype_full_name,
        ast.is_normally_debit as subtype_normally_debit,
        case
            when a.sub_account = '' then a.main_account
            else concat(a.main_account, '-', a.sub_account)
        end as combined_account

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
        case when min(value_type) = 'NET QTY AMT' then min(report_header_category) end as los_volume_report_header,

        -- Separate line numbers for report sequencing (Power BI sort order)
        case when min(value_type) = 'NET QTY AMT' then min(los_mapping_id) end as los_volume_line_number,
        case when max(value_type) = 'NET VALUE AMT' then max(los_mapping_id) end as los_value_line_number,
        max(los_mapping_id) as los_line_number,
        case when min(value_type) = 'NET QTY AMT' then min(los_mapping_id) end as los_volume_report_header_line_number,
        max(line_header_line_number) as los_report_header_line_number,

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

final as (
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
        lm.los_line_item_name,

        lm.los_product_type,

        lm.los_volume_report_header,

        -- =================================================================
        -- LOS Attributes (from SharePoint)
        -- =================================================================
        lm.los_volume_line_number,
        lm.los_value_line_number,
        lm.los_volume_report_header_line_number,
        lm.los_line_number,
        lm.has_volume_reporting,
        lm.has_value_reporting,
        lm.is_los_subtraction,
        lm.is_los_calculated,
        case
            when ah.subtype_code = 'R' then 'REVENUE'
            when ah.subtype_code = 'X' then 'EXPENSE'
            when ah.subtype_code = 'A' then 'ASSET'
            when ah.subtype_code = 'L' then 'LIABILITY'
            when ah.subtype_code = 'E' then 'EQUITY'
            else 'OTHER'
        end as financial_statement_type,
        coalesce(ah.subtype_code in ('R', 'X'), false) as is_income_statement_account,
        coalesce(ah.type_code = 'B', false) as is_balance_sheet_account,
        case when ah.account_code = '900 / 305' then 'COMPANY LABOR' else lm.los_key_sort end as los_key_sort,
        case when ah.account_code = '900 / 305' then 'COMPANY LABOR' else lm.los_report_header end as los_report_header,
        case when ah.account_code = '900 / 305' then 45 else lm.los_report_header_line_number end
            as los_report_header_line_number,

        -- LOS account flag (is this account in the LOS mapping?)
        coalesce(lm.account_code is not null, false) as is_los_account,

        -- =================================================================
        -- LOS Groupings (from SharePoint mapping)
        -- =================================================================

        -- High-level category for broad rollups
        case
            when lm.los_report_header in ('DEV CAPEX', 'MIDSTREAM CAPEX') then 'Capital Expenses'
            when lm.los_report_header in ('MIDSTREAM DEAL COSTS') then 'Midstream'
            when lm.los_report_header = 'O&G LEASEHOLD' then 'Leasehold'
            when
                lm.los_report_header in (
                    'OIL REVENUE',
                    'GAS REVENUE',
                    'NGL REVENUE',
                    'OIL REVENUE DEDUCTS',
                    'GAS REVENUE DEDUCTS',
                    'NGL REVENUE DEDUCTS'
                )
                then 'Revenue'
            -- noqa: LT05
            -- WHEN lm.los_report_header IN ('...DEDUCTS') THEN 'Revenue Deductions'
            when
                lm.los_report_header in (
                    'OIL PRODUCTION TAXES', 'GAS PRODUCTION TAXES', 'NGL PRODUCTION TAXES', 'AD VAL TAXES'
                )
                then 'Production & Ad Valorem Taxes'
            when lm.los_report_header in ('OVERHEAD INCOME', 'WELL SERVICING INCOME', 'MISC INCOME') then 'Other Income'
            when lm.los_report_header = 'WORKOVER EXPENSES' then 'Workover Expenses'
            when lm.los_report_header = 'P&A' then 'P & A Expenses'
            when lm.los_report_header in ('HEDGE SETTLEMENTS', 'CANCELED HEDGES') then 'Derivatives'
            when lm.los_report_header in (
                'LEASE MAINTENANCE', 'SERVICES & REPAIRS', 'SURFACE EQUIPMENT', 'WELL SERVICING & DH EQUIP',
                'COMPANY LABOR', 'CONTRACT LABOR & SUPERVISION', 'CHEMICALS & TREATING', 'RENTAL EQUIPMENT',
                '3RD PTY WTR & DSPL', 'COMPANY WTR & DISPOSAL', 'FUEL & POWER', 'WEATHER',
                'COPAS OVERHEAD', 'NON-OP LOE', 'MIDSTREAM GL INJECTION', 'OTHER'
            ) then 'Lease Operating Expenses'
            when lm.los_line_item_name = 'ACCRUED LOE' then 'Lease Operating Expenses'
            when lm.los_report_header in (
                'CMPNY PR & BNFT', 'CNSL & CNTR EMP', 'HARDWR & SOFTWR', 'OFFICE RENT',
                'CORP FEES', 'CORP INSURANCE', 'AUDIT', 'LEGAL', 'REAL PROP TAX',
                'TRAVEL', 'UTIL & INTERNET', 'VEHICLES', 'SUPPLIES & EQP', 'MISCELLANEOUS'
            ) then 'G&A'
            when lm.los_report_header = 'INVENTORY' then 'Inventory'
        end as los_category,

        -- Detail section (direct from SharePoint report header)
        case
            when account_full_name = '900 / 305: FIELD OFFICE EXPENSE' then 'COMPANY LABOR'
            else lm.los_report_header
        end as los_section,

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
            when lm.los_key_sort in ('WORKOVER', 'CAP PROD/WKOVR') then 'WORKOVER'
            when lm.los_key_sort = 'P&A' then 'ABANDONMENT'
            when lm.los_key_sort = 'HEDGES' then 'DERIVATIVE'
            when lm.los_key_sort is not null and ah.subtype_code = 'R' then 'REVENUE'
            when lm.los_key_sort is not null and ah.subtype_code = 'X' then 'LOE'
            when ah.main_account in ('310', '328', '301') then 'CAPITAL'
            when ah.subtype_code = 'R' then 'REVENUE'
            when ah.subtype_code = 'X' then 'OPERATING'
            else 'OTHER'
        end as expense_classification,

        -- =================================================================
        -- Metadata
        -- =================================================================
        current_timestamp() as _refreshed_at

    from accounts_hierarchy ah
    left join los_mapping lm
        on ah.account_code = lm.account_code
)

select
    *,
    -- =================================================================
    -- LOS Category Sort order
    -- =================================================================        
    case
        when los_category = 'Revenue' then 1
        when los_category = 'Other Income' then 2
        when los_category = 'Production & Ad Valorem Taxes' then 3
        when los_category = 'Workover Expenses' then 4
        when los_category = 'P & A Expenses' then 5
        when los_category = 'Lease Operating Expenses' then 6
        when los_category = 'Capital Expenses' then 7
        when los_category = 'Leasehold' then 8
        when los_category = 'Midstream' then 9
        when los_category = 'Inventory' then 10
        when los_category = 'Derivatives' then 11
        when los_category = 'G&A' then 12
    end as los_category_line_number

from final
order by los_line_number
