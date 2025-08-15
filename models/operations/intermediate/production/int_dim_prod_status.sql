{{ config(
    enable= false,
    materialized='table',
    tags=['marts', 'dim']
) }}

WITH prodstatus as (
    Select
        "Prod Status"
        ,"Prod Method"
        ,count("Prod Status")
        --,"Status Record ID"
    FROM {{ ref('int_prodview__production_volumes') }}
    GROUP BY "Prod Status", "Prod Method"
    --, "Status Record ID"
)

Select *
from prodstatus
order by "Prod Status" asc
