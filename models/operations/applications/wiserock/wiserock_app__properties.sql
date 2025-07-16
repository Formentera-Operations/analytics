{{ config(
    tags=['wiserock', 'combo_curve']
) }}

select * from {{ ref('corporate_wells') }}