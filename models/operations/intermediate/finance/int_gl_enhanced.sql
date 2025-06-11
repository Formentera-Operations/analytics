select 
    gl.*,
    
    -- Account information with LOS classifications
    acc.account_code,
    acc.account_name,
    acc.account_full_name,
    acc.financial_statement_type,
    acc.los_line_item,
    acc.income_statement_section,
    acc.interest_type,
    acc.commodity_type,
    acc.expense_type,
    acc.detailed_subcategory,
    acc.los_sort_order,
    acc.los_value_type,
    acc.is_income_statement_account,
    acc.is_los_account,
    acc.is_capital_account,
    acc.account_normally_debit,
    
    -- Enhanced date fields
    date_trunc('month', gl.journal_date) as journal_month,
    date_trunc('quarter', gl.journal_date) as journal_quarter,
    date_trunc('year', gl.journal_date) as journal_year,
    extract(year from gl.journal_date) as fiscal_year,
    extract(quarter from gl.journal_date) as fiscal_quarter,
    extract(month from gl.journal_date) as fiscal_month,
    
    -- Business logic flags
    case 
        when gl.is_posted = true 
            and gl.net_value != 0 
            and acc.is_income_statement_account = true
        then true
        else false
    end as include_in_income_statement,
    
    -- LOS-specific inclusion flag
    case 
        when gl.is_posted = true 
            and gl.net_value != 0 
            and acc.is_los_account = true
        then true
        else false
    end as include_in_los,
    
    -- Transaction categorization based on description
    case 
        when upper(gl.description) like '%REVERSAL%' or upper(gl.description) like '%REVERSE%' then 'Reversal Entry'
        when upper(gl.description) like '%ACCRUAL%' or upper(gl.description) like '%ACCR%' then 'Accrual Entry'
        when upper(gl.description) like '%PAYROLL%' then 'Payroll'
        when upper(gl.description) like '%COPAS%' then 'Joint Interest'
        when upper(gl.description) like '%OIL%' then 'Oil Related'
        when upper(gl.description) like '%GAS%' then 'Gas Related'
        when upper(gl.description) like '%NGL%' or upper(gl.description) like '%LIQUID%' then 'NGL Related'
        when upper(gl.description) like '%HEDGE%' then 'Hedging'
        else 'Standard Entry'
    end as transaction_category
    
from {{ ref('stg_oda__gl') }} gl
join {{ ref('int_accounts_classified') }} acc 
    on gl.account_id = acc.account_id