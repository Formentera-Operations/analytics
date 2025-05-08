{{
    config(
        materialized='table',
        cluster_by=['scenario_id']
    )
}}

with stg_projects as (
    select * from {{ ref('stg_cc__scenarios') }}
),

projects as (

    select
        scenario_id,
        project_id,
        scenario_name,
        created_at,
        updated_at

    from

        stg_projects
)

select * from projects