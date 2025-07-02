{{ config(
    materialized='view',
    schema='wiserock_app',
    tags=['wiserock', 'wellview']
) }}

select * from {{ ref('stg_wellview__system_integrations') }}
