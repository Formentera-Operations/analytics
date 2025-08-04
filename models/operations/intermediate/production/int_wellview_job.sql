 {{
    config(
        enable=true,
        materialized='view'
    )
}}

with jobs as (
    select
        *
    from {{ ref('stg_wellview__jobs') }}
)

select
    *
from jobs