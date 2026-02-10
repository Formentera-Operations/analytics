{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'facts']
) }}

WITH downtimes as (
    Select
        *
    FROM {{ ref('int_prodview__completion_downtimes') }}
)

Select *
from downtimes
WHERE "First Day" > LAST_DAY(DATEADD(year, -3,CURRENT_DATE()), year)
ORDER BY "First Day"