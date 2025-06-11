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

los_classifications as (
    select *,
        -- High-level P&L classification based on confirmed hierarchy
        case 
            when subtype_code = 'R' then 'REVENUE'
            when subtype_code = 'X' then 'EXPENSE' 
            when type_code = 'B' then 'BALANCE_SHEET'
            else 'OTHER'
        end as financial_statement_type,
        
        -- ====== LOS-BASED REVENUE CLASSIFICATIONS ======
        case 
            -- Oil Revenue (LOS Line 9)
            when subtype_code = 'R' and (
                account_code like '701%' or
                account_code in ('705 / 3', '705 / 4', '705 / 6', '706 / 1') or
                account_code like '840 / 1%' or account_code like '840 / 3%' or account_code like '840 / 5%' or account_code = '840 / 50' or
                account_code like '850 / 1%' or account_code = '850 / 6' or account_code = '850 / 10'
            ) then 'Oil Revenue'
            
            -- Gas Revenue (LOS Line 10) 
            when subtype_code = 'R' and (
                account_code like '702%' or
                account_code in ('705 / 7', '705 / 8', '705 / 10', '705 / 15', '706 / 2') or
                account_code like '840 / 7%' or account_code like '840 / 8%' or account_code like '840 / 9%' or 
                account_code like '840 / 1_' or account_code like '840 / 2_' or account_code like '840 / 3_' or account_code = '840 / 51' or
                account_code like '860 / 1%' or account_code like '860 / 2%' or account_code like '860 / 3%' or 
                account_code like '860 / 4%' or account_code like '860 / 5%' or account_code like '860 / 6%' or account_code = '860 / 10' or
                account_code like '861 / 2%' or account_code like '861 / 4%'
            ) then 'Gas Revenue'
            
            -- NGL Revenue (LOS Line 11)
            when subtype_code = 'R' and (
                account_code like '703%' or
                account_code in ('705 / 11', '705 / 12', '705 / 14', '705 / 16', '706 / 3') or
                account_code like '840 / 25%' or account_code like '840 / 27%' or account_code like '840 / 29%' or account_code = '840 / 52' or
                account_code like '870 / 1%' or account_code like '870 / 2%' or account_code like '870 / 3%' or 
                account_code like '870 / 4%' or account_code like '870 / 5%' or account_code like '870 / 6%' or account_code = '870 / 10'
            ) then 'NGL Revenue'
            
            -- Overhead Income (LOS Line 12)
            when subtype_code = 'R' and (
                account_code like '704%' or
                account_code in ('715 / 2', '715 / 3', '715 / 4', '715 / 7', '715 / 13', '715 / 14', '715 / 15', '715 / 16', '715 / 17', '715 / 20', '715 / 23')
            ) then 'Overhead Income'
            
            -- Misc Income (LOS Line 14)
            when subtype_code = 'R' and (
                account_code in ('715 / 5', '715 / 6', '715 / 8', '715 / 9', '715 / 10', '715 / 11', '715 / 12', '715 / 18', '715 / 21', '715 / 22', '705 / 999')
            ) then 'Miscellaneous Income'
            
            -- Other Revenue
            when subtype_code = 'R' then 'Other Revenue'
            
            -- ====== LOS-BASED EXPENSE CLASSIFICATIONS ======
            -- Production and Ad Valorem Taxes (LOS Line 18)
            when subtype_code = 'X' and (
                account_code like '830%' or account_code like '831%' or account_code like '905%'
            ) then 'Production & Ad Valorem Taxes'
            
            -- Workover Expenses (LOS Line 21)
            when subtype_code = 'X' and account_code like '903%' then 'Workover Expenses'
            
            -- Lease Maintenance (LOS Line 22)
            when subtype_code = 'X' and (
                account_code like '900 / 1%' or account_code like '900 / 2%' or 
                account_code like '900 / 10_' or account_code like '901 / 1%'
            ) then 'Lease Maintenance'
            
            -- Water & Disposal (LOS Lines 23-24)
            when subtype_code = 'X' and (
                account_code like '900 / 4%' or account_code like '900 / 5%' or account_code like '900 / 6%' or
                account_code like '900 / 11_' or account_code like '900 / 31_' or 
                account_code like '900 / 36%' or account_code like '900 / 98_' or account_code like '900 / 99_' or
                account_code like '901 / 4%' or account_code like '901 / 6%' or account_code like '904 / 98_' or account_code like '904 / 99_'
            ) then 'Water & Disposal'
            
            -- Gas Lift Injection (LOS Line 25)
            when subtype_code = 'X' and (
                account_code like '900 / 35%' or account_code like '900 / 31_' or 
                account_code like '900 / 985%' or account_code like '904 / 985%'
            ) then 'Gas Lift Injection'
            
            -- Weather (LOS Line 26)
            when subtype_code = 'X' and (
                account_code like '900 / 12_' or account_code like '900 / 99_' or account_code like '904 / 99_'
            ) then 'Weather Operations'
            
            -- Equipment & Services (LOS Lines 27-29, 33)
            when subtype_code = 'X' and (
                account_code like '900 / 2_' or account_code like '900 / 13_' or account_code like '900 / 14_' or 
                account_code like '900 / 15_' or account_code like '900 / 16_' or account_code like '900 / 17_' or
                account_code like '900 / 18_' or account_code like '900 / 19_' or account_code like '900 / 20_' or
                account_code like '900 / 21%' or account_code like '900 / 22%' or account_code like '900 / 23%'
            ) then 'Equipment & Services'
            
            -- Fuel & Power (LOS Line 30)
            when subtype_code = 'X' and (
                account_code like '900 / 175%' or account_code like '900 / 176%' or account_code like '900 / 177%' or
                account_code like '900 / 20%' or account_code like '900 / 157%' or account_code like '900 / 996%' or
                account_code like '901 / 20%' or account_code like '901 / 176%' or account_code like '904 / 996%'
            ) then 'Fuel & Power'
            
            -- Chemicals & Treating (LOS Line 31)
            when subtype_code = 'X' and (
                account_code like '900 / 14%' or account_code like '900 / 15%' or account_code like '900 / 16%' or
                account_code like '900 / 18_' or account_code like '901 / 14%' or account_code like '901 / 15%'
            ) then 'Chemicals & Treating'
            
            -- Contract Labor (LOS Line 32)
            when subtype_code = 'X' and (
                account_code like '900 / 41%' or account_code like '900 / 42%' or account_code like '900 / 43%' or
                account_code like '900 / 19_' or account_code like '900 / 983%' or account_code like '901 / 41%' or account_code like '901 / 19_'
            ) then 'Contract Labor & Supervision'
            
            -- Company Labor (LOS Line 34)
            when subtype_code = 'X' and (
                account_code like '900 / 30_' or account_code like '802 / 14%' or account_code like '802 / 16%' or
                account_code like '900 / 53%' or account_code like '900 / 55%' or account_code like '900 / 31_' or account_code like '900 / 32_'
            ) then 'Company Labor & Overhead'
            
            -- Non-Op LOE (LOS Line 35)
            when subtype_code = 'X' and (
                account_code like '900 / 70%' or account_code like '900 / 308%' or 
                account_code like '900 / 986%' or account_code like '904 / 986%'
            ) then 'Non-Operated LOE'
            
            -- COPAS Overhead (LOS Line 37)
            when subtype_code = 'X' and (
                account_code like '900 / 48%' or account_code like '900 / 309%' or account_code like '900 / 989%' or
                account_code like '901 / 309%' or account_code like '904 / 989%'
            ) then 'COPAS Overhead'
            
            -- P&A (LOS Line 39)
            when subtype_code = 'X' and account_code like '806%' then 'Plugging & Abandonment'
            
            -- Development CAPEX (LOS Line 53)
            when subtype_code = 'X' and account_code like '310%' then 'Development CAPEX'
            
            -- Midstream CAPEX (LOS Line 54)
            when subtype_code = 'X' and account_code like '328%' then 'Midstream CAPEX'
            
            -- Hedge Settlements (LOS Line 52)
            when subtype_code = 'X' and account_code like '935%' then 'Hedge Settlements'
            
            -- Leasehold & Land (LOS Line 55)
            when subtype_code = 'X' and account_code like '301%' then 'Leasehold & Land Costs'
            
            -- Other Expenses
            when subtype_code = 'X' then 'Other Operating Expenses'
            
            else 'Other'
        end as los_line_item,
        
        -- LOS-based income statement grouping
        case 
            when subtype_code = 'R' then 'Revenue'
            when subtype_code = 'X' and account_code like '830%' then 'Production Taxes'
            when subtype_code = 'X' and account_code like '900%' then 'Lease Operating Expenses'
            when subtype_code = 'X' and account_code like '903%' then 'Workover Expenses'
            when subtype_code = 'X' and account_code like '905%' then 'Ad Valorem Taxes'
            when subtype_code = 'X' and account_code like '806%' then 'Abandonment Costs'
            when subtype_code = 'X' and account_code like '310%' then 'Development Capital'
            when subtype_code = 'X' and account_code like '328%' then 'Midstream Capital'
            when subtype_code = 'X' and account_code like '301%' then 'Leasehold Acquisition'
            when subtype_code = 'X' and account_code like '935%' then 'Derivative Settlements'
            else 'Other'
        end as income_statement_section,
        
        -- Interest type classification (for revenue accounts)
        case 
            when account_name ilike '%WI%' or account_name ilike '%working%' or account_code like '%/ 1' then 'Working Interest'
            when account_name ilike '%RI%' or account_name ilike '%royalty%' or account_code like '%/ 2' then 'Royalty Interest'
            when account_name ilike '%ORRI%' or account_name ilike '%overriding%' or account_code like '%/ 3' then 'Overriding Royalty'
            when account_name ilike '%hedge%' or account_code like '%/ 4' or account_code like '%/ 2_' then 'Hedging'
            when account_name ilike '%accrued%' or account_code like '%/ 5' then 'Accruals'
            when account_name ilike '%deduct%' or account_code like '84_%' or account_code like '85_%' or account_code like '86_%' or account_code like '87_%' then 'Deductions'
            else 'Base'
        end as interest_type,
        
        -- Commodity classification (for revenue accounts)
        case 
            when account_code like '701%' then 'Oil'
            when account_code like '702%' then 'Gas'
            when account_code like '703%' then 'NGL'
            else 'Non-Commodity'
        end as commodity_type,
        
        -- LOS line number for sorting
        case 
            -- Revenue items
            when account_code like '701%' or los_line_item = 'Oil Revenue' then 9
            when account_code like '702%' or los_line_item = 'Gas Revenue' then 10
            when account_code like '703%' or los_line_item = 'NGL Revenue' then 11
            when los_line_item = 'Overhead Income' then 12
            when los_line_item = 'Miscellaneous Income' then 14
            
            -- Expense items
            when los_line_item = 'Production & Ad Valorem Taxes' then 18
            when los_line_item = 'Workover Expenses' then 21
            when los_line_item = 'Lease Maintenance' then 22
            when los_line_item = 'Water & Disposal' then 23
            when los_line_item = 'Gas Lift Injection' then 25
            when los_line_item = 'Weather Operations' then 26
            when los_line_item = 'Equipment & Services' then 27
            when los_line_item = 'Fuel & Power' then 30
            when los_line_item = 'Chemicals & Treating' then 31
            when los_line_item = 'Contract Labor & Supervision' then 32
            when los_line_item = 'Company Labor & Overhead' then 34
            when los_line_item = 'Non-Operated LOE' then 35
            when los_line_item = 'COPAS Overhead' then 37
            when los_line_item = 'Plugging & Abandonment' then 39
            when los_line_item = 'Hedge Settlements' then 52
            when los_line_item = 'Development CAPEX' then 53
            when los_line_item = 'Midstream CAPEX' then 54
            when los_line_item = 'Leasehold & Land Costs' then 55
            
            else 999
        end as los_sort_order,
        
        -- Value type for LOS reporting
        case 
            when subtype_code = 'R' then 'REVENUE_VALUE'
            when subtype_code = 'X' then 'EXPENSE_VALUE'
            else 'OTHER_VALUE'
        end as los_value_type,
        
        -- Operating vs Capital classification
        case 
            when account_code like '310%' or account_code like '328%' or account_code like '301%' then 'CAPITAL'
            when account_code like '806%' then 'ABANDONMENT'
            when account_code like '935%' then 'DERIVATIVE'
            when subtype_code = 'R' then 'REVENUE'
            when subtype_code = 'X' then 'OPERATING'
            else 'OTHER'
        end as expense_type,
        
        -- Detailed subcategory for enhanced reporting
        case 
            -- Oil Revenue subcategories
            when account_code = '701' then 'Oil Sales - Header'
            when account_code = '701 / 1' then 'Oil Sales - Working Interest'
            when account_code = '701 / 2' then 'Oil Sales - Royalty Interest'
            when account_code = '701 / 3' then 'Oil Sales - ORRI'
            when account_code = '701 / 4' then 'Oil Hedging'
            when account_code = '701 / 5' then 'Accrued Oil Sales'
            
            -- Gas Revenue subcategories
            when account_code = '702' then 'Gas Sales - Header'
            when account_code = '702 / 1' then 'Gas Sales - Working Interest'
            when account_code = '702 / 2' then 'Gas Sales - Royalty Interest'
            when account_code = '702 / 3' then 'Gas Sales - ORRI'
            when account_code = '702 / 4' then 'Gas Hedging'
            when account_code = '702 / 5' then 'Accrued Gas Sales'
            when account_code = '702 / 6' then 'Firm Transportation Used'
            when account_code like '702 / 7%' or account_code like '702 / 8%' or account_code like '702 / 9%' then 'Lease Gas Sales'
            when account_code like '702 / 10%' or account_code like '702 / 11%' or account_code like '702 / 12%' then 'Flared Gas Sales'
            when account_code = '702 / 990' then 'Gas Adjustment'
            
            -- NGL Revenue subcategories
            when account_code = '703' then 'NGL Sales - Header'
            when account_code = '703 / 1' then 'NGL Sales - Working Interest'
            when account_code = '703 / 2' then 'NGL Sales - Royalty Interest'
            when account_code = '703 / 3' then 'NGL Sales - ORRI'
            when account_code = '703 / 5' then 'Accrued NGL Sales'
            when account_code like '703 / 6%' or account_code like '703 / 7%' or account_code like '703 / 8%' or 
                 account_code like '703 / 9%' or account_code like '703 / 10%' or account_code like '703 / 11%' then 'NGL Product Sales'
            when account_code like '703 / 2_%' then 'NGL Hedging'
            
            -- Production Tax subcategories
            when account_code like '830 / 1%' or account_code like '830 / 3%' or account_code like '830 / 5%' then 'Oil Production Taxes'
            when account_code like '830 / 7%' or account_code like '830 / 9%' or account_code like '830 / 11%' then 'Gas Production Taxes'
            when account_code like '830 / 13%' or account_code like '830 / 14%' or account_code like '830 / 15%' then 'NGL Production Taxes'
            when account_code like '830 / 2%' or account_code like '830 / 4%' or account_code like '830 / 6%' or 
                 account_code like '830 / 8%' or account_code like '830 / 10%' or account_code like '830 / 12%' then 'Regulatory Fees'
            when account_code like '830 / 16%' or account_code like '830 / 17%' or account_code like '830 / 18%' then 'Accrued Production Taxes'
            
            -- LOE subcategories by major expense type
            when account_code like '900 / 1%' then 'Road & Lease Maintenance'
            when account_code like '900 / 4%' or account_code like '900 / 5%' or account_code like '900 / 6%' then 'Water & Disposal Services'
            when account_code like '900 / 14%' or account_code like '900 / 15%' or account_code like '900 / 16%' then 'Chemicals & Treating'
            when account_code like '900 / 20%' or account_code like '900 / 175%' or account_code like '900 / 176%' then 'Power & Fuel'
            when account_code like '900 / 21%' or account_code like '900 / 22%' or account_code like '900 / 23%' then 'Well Servicing'
            when account_code like '900 / 24%' or account_code like '900 / 25%' then 'Equipment Rentals'
            when account_code like '900 / 30_%' then 'Company Labor & Supervision'
            when account_code like '900 / 41%' or account_code like '900 / 42%' or account_code like '900 / 43%' then 'Contract Services'
            when account_code like '900 / 48%' or account_code like '900 / 309%' then 'COPAS Overhead'
            when account_code like '900 / 70%' then 'Non-Operated Expenses'
            
            else account_name
        end as detailed_subcategory,
        
        -- Flag for income statement relevance (confirmed mapping)
        case 
            when subtype_code in ('R', 'X') then true
            else false
        end as is_income_statement_account,
        
        -- Flag for LOS relevance
        case 
            when los_sort_order != 999 then true
            else false
        end as is_los_account,
        
        -- Flag for capital vs operating
        case 
            when expense_type = 'CAPITAL' then true
            else false
        end as is_capital_account
        
    from accounts_hierarchy
)

select * from los_classifications