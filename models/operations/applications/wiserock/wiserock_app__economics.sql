{{ config(
    tags=['wiserock', 'combo_curve']
) }}

select * from {{ ref('corporate_economic_runs_with_one_liners') }}