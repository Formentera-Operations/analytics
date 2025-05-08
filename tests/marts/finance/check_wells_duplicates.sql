-- This test will return any well_id that appears more than once in the wells model
with duplicate_check as (
    select 
        well_id,
        count(*) as occurrence_count
    from {{ ref('wells') }}
    group by well_id
    having count(*) > 1
)

select
    well_id,
    occurrence_count
from duplicate_check