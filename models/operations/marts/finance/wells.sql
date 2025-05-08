with unioned_wells as (
    select *
    from {{ ref('project_wells') }}
    
    union all
    
    select *
    from {{ ref('company_wells') }}
),

deduplicated_wells as (
    select *,
           row_number() over (partition by well_id order by updated_at desc) as rn
    from unioned_wells
)

select 
    *
from deduplicated_wells
where rn = 1