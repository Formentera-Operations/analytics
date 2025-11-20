{{
    config(
        materialized='table',
        tags=['aegis', 'intermediate', 'hedging', 'analytics']
    )
}}


    select * from {{ ref('stg_aegis__market_data') }}


