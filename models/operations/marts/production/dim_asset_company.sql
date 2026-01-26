{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'dim']
) }}

WITH companies as (
    Select
        *
    FROM {{ ref('int_dim_asset_company') }}
)

Select *
from companies