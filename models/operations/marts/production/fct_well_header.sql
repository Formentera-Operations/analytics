{{ config(
    enable= true,
    materialized='table',
    tags=['marts', 'facts']
) }}

WITH wellheader as (
    Select
        *
    FROM {{ ref('int_fct_well_header') }}
)

Select *
from wellheader