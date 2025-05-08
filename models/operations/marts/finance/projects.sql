{{
    config(
        materialized='table',
        cluster_by=['project_id']
    )
}}

with stg_projects as (
    select * from {{ ref('stg_cc__projects') }}
),

projects as (

    select

        project_id,
        project_name,
        created_at,
        updated_at

    from

        stg_projects
)

select * from projects