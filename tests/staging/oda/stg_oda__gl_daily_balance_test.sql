
-- tests/gl_daily_balance.sql
-- This test will FAIL if it returns any rows (that's how dbt tests work)

SELECT 
    journal_date,
    company_id,
    COUNT(*) as entry_count,
    SUM(CASE WHEN net_value > 0 THEN net_value ELSE 0 END) as total_debits,
    SUM(CASE WHEN net_value < 0 THEN ABS(net_value) ELSE 0 END) as total_credits,
    ABS(SUM(net_value)) as imbalance
FROM {{ ref('stg_oda__gl') }}
WHERE is_posted = true
  AND journal_date = current_date - 1  -- Check yesterday's complete data
GROUP BY journal_date, company_id
HAVING ABS(SUM(net_value)) > 0.01  -- Only return unbalanced companies