{{ config(
    enable= false,
    materialized='table',
    tags=['marts', 'facts']
) }}

with contracts as (
    select
        *
    from {{ ref('int_aegis__shrink_and_yield') }}
)

select * from contracts