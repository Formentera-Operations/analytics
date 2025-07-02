{{ config(
    tags=['wiserock', 'prodview']
) }}

select * from {{ ref('stg_prodview__completions') }}