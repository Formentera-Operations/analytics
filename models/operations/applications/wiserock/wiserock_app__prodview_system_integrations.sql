{{ config(
    materialized='view',
    schema='wiserock_app',
    tags=['wiserock', 'prodview']
) }}

select * from {{ ref('stg_prodview__system_integrations') }}