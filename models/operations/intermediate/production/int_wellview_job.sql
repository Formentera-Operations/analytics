 {{
    config(
        enabled=true,
        materialized='view'
    )
}}

with jobs as (
    select
        *
    from {{ ref('stg_wellview__jobs') }}
)
,
wells as (
    select
        *
    from {{ ref('stg_wellview__well_header') }}
)

select
    jobs.*
    ,wells."Cost Center" as "Well Code"
    ,wells."Well Type"
    ,wells."API 10 Number"
from jobs
left join wells
on jobs."Well ID" = wells."Well ID"