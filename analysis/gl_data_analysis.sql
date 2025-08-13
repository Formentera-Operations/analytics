with source as (
    select * from {{ ref('stg_oda__gl') }}
)

-- Date analysis
select
    min(journal_date) as min_journal_date,
    max(journal_date) as max_journal_date,
    min(accrual_date) as min_accrual_date,
    max(accrual_date) as max_accrual_date,
    min(cash_date) as min_cash_date,
    max(cash_date) as max_cash_date,
    min(journal_date_key) as min_journal_date_key,
    max(journal_date_key) as max_journal_date_key
from source
where journal_date is not null
  and accrual_date is not null
  and cash_date is not null;

-- Currency analysis
select distinct currency_id, count(*) as count
from source
group by 1
order by 2 desc;

-- Source module analysis
select distinct source_module, count(*) as count
from source
group by 1
order by 2 desc;