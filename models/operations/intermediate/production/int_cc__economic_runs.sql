{{ config(
    enable= false,
    materialized='view'
) }}

WITH strip as (
    select *
    from {{ ref('stg_cc__economic_runs') }}
        WHERE DATE > '2022-12-31'
        AND DATE < CURRENT_DATE()
),

project as (
    select *
    from {{ ref('stg_cc__projects') }}
),

scenario as (
    select *
    from {{ ref('stg_cc__scenarios') }}
)

select 
    --ID,
    --EXTRACTED_AT,
    --ECON_RUN,
    --ECON_RUN_DATE,
    DATE,
    COMBO_NAME,
    --WELL_ID,
    --PROJECT,
    project_name,
    --SCENARIO,
    scenario_name,
    --ECONOMIC_RUN_WELL_ID,
    OIL_PRICE,
    GAS_PRICE,
    NGL_PRICE,
    DRIP_CONDENSATE_PRICE,
    --INPUT_OIL_PRICE,
    --INPUT_GAS_PRICE,
    --INPUT_NGL_PRICE,
    --INPUT_DRIP_CONDENSATE_PRICE,
    --OIL_START_USING_FORECAST_DATE,
    --GAS_START_USING_FORECAST_DATE,
    --WATER_START_USING_FORECAST_DATE
from strip s
left join project p
on s.PROJECT = p.project_id
left join scenario ss
on s.SCENARIO = ss.scenario_id
GROUP BY ALL
 ORDER BY DATE ASC