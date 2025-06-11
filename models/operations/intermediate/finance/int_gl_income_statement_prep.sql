select *,
    -- Apply proper accounting sign convention for P&L
    case 
        when financial_statement_type = 'REVENUE' then net_value * -1  -- Revenue: credit balance to positive
        when financial_statement_type = 'EXPENSE' then net_value       -- Expense: debit balance stays positive
        else net_value
    end as income_statement_amount,
    
    -- LOS-specific amount calculation (same logic but explicit)
    case 
        when financial_statement_type = 'REVENUE' then net_value * -1
        when financial_statement_type = 'EXPENSE' then net_value
        else net_value
    end as los_amount,
    
    -- Absolute amounts for analysis
    abs(net_value) as net_value_absolute,
    
    -- Amount size categorization
    case 
        when abs(net_value) >= 1000000 then 'Large (>$1M)'
        when abs(net_value) >= 10000 then 'Medium ($10K-$1M)'
        when abs(net_value) >= 100 then 'Small ($100-$10K)'
        when abs(net_value) > 0 then 'Minimal (<$100)'
        else 'Zero'
    end as amount_size_category,
    
    -- LOS section classification for reporting
    case 
        when los_sort_order between 1 and 15 then 'REVENUE'
        when los_sort_order between 17 and 40 then 'OPERATING_EXPENSES'
        when los_sort_order between 50 and 60 then 'OTHER_ITEMS'
        else 'OTHER'
    end as los_section,
    
    -- Capital vs Operating classification
    case 
        when is_capital_account then 'CAPITAL'
        when expense_type = 'ABANDONMENT' then 'ABANDONMENT'
        when expense_type = 'DERIVATIVE' then 'DERIVATIVE'
        when financial_statement_type = 'REVENUE' then 'REVENUE'
        else 'OPERATING'
    end as financial_classification
    
from {{ ref('int_gl_enhanced') }}