with accounts_hierarchy as (
    select 
        a.id as account_id,
        a.code as account_code,
        a.name as account_name,
        a.full_name as account_full_name,
        a.main_account,
        a.sub_account,
        a.active,
        a.normally_debit as account_normally_debit,
        
        -- Join complete hierarchy
        at.type_code,
        at.type_name,
        at.type_full_name,
        ast.subtype_code,
        ast.subtype_name,
        ast.subtype_full_name,
        ast.normally_debit as subtype_normally_debit
        
    from {{ ref('stg_oda__account_v2') }} a
    join {{ ref('stg_oda__account_sub_types') }} ast 
        on a.account_subtype_id = ast.account_subtype_id
    join {{ ref('stg_oda__account_types') }} at 
        on ast.account_type_id = at.account_type_id
    where a.active = true
),

los_mapping as (
    select * from {{ ref('seed_los_account_mapping') }}
),

los_classifications as (
    select 
        ah.*,
        
        -- LOS mapping from seed file (handle nulls for unmapped accounts)
        lm.key_sort as los_category,
        lm.category_full_name as los_category_name,
        lm.category_type as los_category_type,
        lm.category_description as los_category_description,
        
        -- High-level P&L classification based on confirmed hierarchy and LOS mapping
        case 
            when lm.category_type = 'Revenue' then 'REVENUE'
            when lm.category_type = 'Expense' then 'EXPENSE'
            when ah.subtype_code = 'R' then 'REVENUE'
            when ah.subtype_code = 'X' then 'EXPENSE' 
            when ah.type_code = 'B' then 'BALANCE_SHEET'
            else 'OTHER'
        end as financial_statement_type,
        
        -- Use LOS mapping for line item classification, fallback for unmapped accounts
        case 
            when lm.category_full_name is not null then lm.category_full_name
            when lm.category_type is null and ah.subtype_code = 'R' then 'Other Revenue'
            when lm.category_type is null and ah.subtype_code = 'X' then 'Other Expenses'
            when lm.category_type is null and ah.type_code = 'A' then 'Assets'
            when lm.category_type is null and ah.type_code = 'L' then 'Liabilities'
            when lm.category_type is null and ah.type_code = 'E' then 'Equity'
            else 'Other'
        end as los_line_item,
        
        -- LOS-based income statement grouping using seed data with fallbacks
        case 
            when lm.key_sort in ('OIL SALES', 'GAS SALES', 'NGL SALES') then 'Commodity Revenue'
            when lm.key_sort in ('OH INCOME', 'WELL SRV INCOME', 'OTHER INCOME') then 'Other Revenue'
            when lm.key_sort in ('OIL PROD TAXES', 'GAS PROD TAXES', 'NGL PROD TAXES') then 'Production Taxes'
            when lm.key_sort in ('OIL REV DEDUCT', 'GAS REV DEDUCT', 'NGL REV DEDUCT') then 'Revenue Deductions'
            when lm.key_sort in ('LEASE MNT & HSE', '3PTY WTR DSPSL', 'CMPNY WTR DSPSL', 'WEATHER', 
                                'RENTAL EQUIP', 'SURFACE EQUIP', 'FUEL & POWER', 'CHEM & TREATING',
                                'CNTRCT LBR', 'COMPANY LABOR', 'WELL SERVICING', 'SRVCS & RPRS',
                                'NON-OP LOE', 'COPAS OVERHEAD', 'AD VALOREM', 'MDSTRM GGA FEE') then 'Lease Operating Expenses'
            when lm.key_sort = 'WORKOVER' then 'Workover Expenses'
            when lm.key_sort = 'P&A' then 'Abandonment Costs'
            when lm.key_sort = 'HEDGES' then 'Derivative Settlements'
            when lm.key_sort = 'OTHER' then 'Other Expenses'
            -- Fallback to original logic for unmapped accounts
            when lm.key_sort is null and ah.subtype_code = 'R' then 'Revenue'
            when lm.key_sort is null and ah.subtype_code = 'X' then 'Operating Expenses'
            when lm.key_sort is null and ah.type_code = 'A' then 'Assets'
            when lm.key_sort is null and ah.type_code = 'L' then 'Liabilities'
            when lm.key_sort is null and ah.type_code = 'E' then 'Equity'
            else 'Other'
        end as income_statement_section,
        
        -- Interest type classification (enhanced with LOS data)
        case 
            when ah.account_name ilike '%WI%' or ah.account_name ilike '%working%' or ah.account_code like '%/ 1' then 'Working Interest'
            when ah.account_name ilike '%RI%' or ah.account_name ilike '%royalty%' or ah.account_code like '%/ 2' then 'Royalty Interest'
            when ah.account_name ilike '%ORRI%' or ah.account_name ilike '%overriding%' or ah.account_code like '%/ 3' then 'Overriding Royalty'
            when ah.account_name ilike '%hedge%' or ah.account_code like '%/ 4' or ah.account_code like '%/ 2_' then 'Hedging'
            when ah.account_name ilike '%accrued%' or ah.account_code like '%/ 5' then 'Accruals'
            when ah.account_name ilike '%deduct%' or ah.account_code like '84_%' or ah.account_code like '85_%' or ah.account_code like '86_%' or ah.account_code like '87_%' then 'Deductions'
            else 'Base'
        end as interest_type,
        
        -- Commodity classification (enhanced with LOS data, handle nulls)
        case 
            when lm.key_sort = 'OIL SALES' then 'Oil'
            when lm.key_sort = 'GAS SALES' then 'Gas'
            when lm.key_sort = 'NGL SALES' then 'NGL'
            -- Fallback logic for unmapped accounts
            when lm.key_sort is null and ah.account_code like '701%' then 'Oil'
            when lm.key_sort is null and ah.account_code like '702%' then 'Gas'
            when lm.key_sort is null and ah.account_code like '703%' then 'NGL'
            else 'Non-Commodity'
        end as commodity_type,
        
        -- LOS line number for sorting (enhanced with comprehensive mapping)
        case 
            -- Revenue items
            when lm.key_sort = 'OIL SALES' then 9
            when lm.key_sort = 'GAS SALES' then 10
            when lm.key_sort = 'NGL SALES' then 11
            when lm.key_sort = 'OH INCOME' then 12
            when lm.key_sort = 'WELL SRV INCOME' then 13
            when lm.key_sort = 'OTHER INCOME' then 14
            
            -- Deduction items
            when lm.key_sort = 'OIL REV DEDUCT' then 15
            when lm.key_sort = 'GAS REV DEDUCT' then 16
            when lm.key_sort = 'NGL REV DEDUCT' then 17
            
            -- Tax items
            when lm.key_sort = 'OIL PROD TAXES' then 18
            when lm.key_sort = 'GAS PROD TAXES' then 19
            when lm.key_sort = 'NGL PROD TAXES' then 20
            
            -- Operating expense items
            when lm.key_sort = 'WORKOVER' then 21
            when lm.key_sort = 'LEASE MNT & HSE' then 22
            when lm.key_sort = '3PTY WTR DSPSL' then 23
            when lm.key_sort = 'CMPNY WTR DSPSL' then 24
            when lm.key_sort = 'MDSTRM GGA FEE' then 25
            when lm.key_sort = 'WEATHER' then 26
            when lm.key_sort = 'RENTAL EQUIP' then 27
            when lm.key_sort = 'SURFACE EQUIP' then 28
            when lm.key_sort = 'SRVCS & RPRS' then 29
            when lm.key_sort = 'FUEL & POWER' then 30
            when lm.key_sort = 'CHEM & TREATING' then 31
            when lm.key_sort = 'CNTRCT LBR' then 32
            when lm.key_sort = 'WELL SERVICING' then 33
            when lm.key_sort = 'COMPANY LABOR' then 34
            when lm.key_sort = 'NON-OP LOE' then 35
            when lm.key_sort = 'COPAS OVERHEAD' then 37
            when lm.key_sort = 'AD VALOREM' then 38
            when lm.key_sort = 'P&A' then 39
            
            -- Other items
            when lm.key_sort = 'HEDGES' then 52
            when lm.key_sort = 'OTHER' then 999
            
            -- Fallback to original logic for unmapped accounts
            when lm.key_sort is null and ah.account_code like '701%' then 9
            when lm.key_sort is null and ah.account_code like '702%' then 10
            when lm.key_sort is null and ah.account_code like '703%' then 11
            when lm.key_sort is null and ah.account_code like '830%' then 18
            when lm.key_sort is null and ah.account_code like '900%' then 30
            when lm.key_sort is null and ah.account_code like '903%' then 21
            when lm.key_sort is null and ah.account_code like '905%' then 38
            when lm.key_sort is null and ah.account_code like '806%' then 39
            when lm.key_sort is null and ah.account_code like '935%' then 52
            
            else 9999
        end as los_sort_order,
        
        -- Value type for LOS reporting
        case 
            when lm.category_type = 'Revenue' then 'REVENUE_VALUE'
            when lm.category_type = 'Expense' then 'EXPENSE_VALUE'
            when subtype_code = 'R' then 'REVENUE_VALUE'
            when subtype_code = 'X' then 'EXPENSE_VALUE'
            else 'OTHER_VALUE'
        end as los_value_type,
        
        -- Operating vs Capital classification (enhanced with LOS data, handle nulls)
        case 
            when lm.key_sort = 'WORKOVER' and ah.account_code like '903%' then 'CAPITAL'
            when lm.key_sort = 'P&A' then 'ABANDONMENT'
            when lm.key_sort = 'HEDGES' then 'DERIVATIVE'
            when lm.category_type = 'Revenue' then 'REVENUE'
            when lm.category_type = 'Expense' then 'OPERATING'
            -- Fallback logic for unmapped accounts
            when lm.key_sort is null and ah.account_code like '310%' then 'CAPITAL'
            when lm.key_sort is null and ah.account_code like '328%' then 'CAPITAL'
            when lm.key_sort is null and ah.account_code like '301%' then 'CAPITAL'
            when lm.key_sort is null and ah.account_code like '806%' then 'ABANDONMENT'
            when lm.key_sort is null and ah.account_code like '935%' then 'DERIVATIVE'
            when lm.key_sort is null and ah.subtype_code = 'R' then 'REVENUE'
            when lm.key_sort is null and ah.subtype_code = 'X' then 'OPERATING'
            else 'OTHER'
        end as expense_type,
        
        -- Detailed subcategory using account name from LOS mapping, fallback to original name
        coalesce(lm.account_name, ah.account_name) as detailed_subcategory,
        
        -- Flag for income statement relevance (handle nulls)
        case 
            when lm.category_type in ('Revenue', 'Expense') then true
            when lm.category_type is null and ah.subtype_code in ('R', 'X') then true
            else false
        end as is_income_statement_account,
        
        -- Flag for LOS relevance (accounts in our seed file)
        case 
            when lm.account_code is not null then true
            else false
        end as is_los_account,
        
        -- Flag for capital vs operating (handle nulls)
        case 
            when lm.key_sort = 'WORKOVER' and ah.account_code like '903%' then true
            when lm.key_sort is null and ah.account_code like '310%' then true
            when lm.key_sort is null and ah.account_code like '328%' then true
            when lm.key_sort is null and ah.account_code like '301%' then true
            else false
        end as is_capital_account
        
    from accounts_hierarchy ah
    left join los_mapping lm 
        on ah.account_code = lm."full_account"
)

select * from los_classifications