with corporate_well_projects as (
    select distinct
        well_id,
        project_id
    from {{ ref('corporate_economic_runs_with_one_liners') }}
)

select 
    w.*,
    cwp.project_id,
    p.project_name
from {{ ref('wells') }} w
inner join corporate_well_projects cwp
    on w.well_id = cwp.well_id
left join {{ ref('projects') }} p
    on cwp.project_id = p.project_id