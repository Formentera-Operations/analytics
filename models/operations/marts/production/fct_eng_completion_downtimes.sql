{{ config(
    enable= true,
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