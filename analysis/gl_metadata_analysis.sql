-- Analysis of GL metadata
with source as (
    select * from {{ ref('stg_oda__gl') }}
)

select distinct currency_id
from source
order by 1;

select distinct source_module
from source
order by 1;

-- Check for duplicate IDs
select id, count(*) as count
from source
group by id
having count(*) > 1
order by count desc
limit 5;