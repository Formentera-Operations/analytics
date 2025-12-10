{{ config(materialized="table") }}

select * from {{ ref("seed__example_data_to_mask") }}
