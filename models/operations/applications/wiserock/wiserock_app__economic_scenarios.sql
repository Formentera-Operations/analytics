{{ config(
    tags=['wiserock', 'combo_curve']
) }}


with corporate_scenarios as (
    select distinct
        scenario_id
    from {{ ref('corporate_economic_runs_with_one_liners') }}
)
select
    scenarios.scenario_id,
    scenarios.scenario_name,
    scenarios.project_id,
    projects.project_name,
    scenarios.updated_at
 from {{ ref('scenarios') }} scenarios
inner join corporate_scenarios
on scenarios.scenario_id = corporate_scenarios.scenario_id
left join {{ ref('projects') }} projects
on scenarios.project_id = projects.project_id