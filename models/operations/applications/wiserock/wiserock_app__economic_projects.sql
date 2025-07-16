{{ config(
    tags=['wiserock', 'combo_curve']
) }}


with corporate_projects as (
    select distinct
        project_id
    from {{ ref('corporate_economic_runs_with_one_liners') }}
)
select
    projects.project_id,
    projects.project_name,
    projects.updated_at
 from {{ ref('projects') }} projects
inner join corporate_projects
on projects.project_id = corporate_projects.project_id