{{ config(
    enable= true,
    materialized='table',
    tags=['marts', 'facts']
) }}

WITH prodvolume as (
    Select
        *
    FROM {{ ref('int_prodview__production_volumes') }}
)

Select *
from prodvolume
where "Prod Date" > '2021-12-31' and "Prod Date" < CAST(GETDATE() AS date) - 1
order by "Prod Date" Desc