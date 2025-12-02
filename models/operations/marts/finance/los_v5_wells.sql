{{ config(
    enabled=true,
    materialized='table'
) }}


    SELECT *
  FROM {{ ref('int_oda_wells') }}
