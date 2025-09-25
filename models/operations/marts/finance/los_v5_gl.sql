{{ config(
    enable= true,
    materialized='table'
) }}

SELECT *
FROM {{ ref('int_oda_gl') }}
    WHERE "JE Date" > '2021-12-31'