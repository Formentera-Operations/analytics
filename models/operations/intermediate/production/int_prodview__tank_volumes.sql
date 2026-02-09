{{
    config(
        enabled=true,
        materialized='view'
    )
}}
with tankvol as (
    select
        *
     From {{ ref('stg_prodview__tank_daily_volumes') }}
     where is_deleted = false
)
,
tanks as (
    select
        *
    from {{ ref('stg_prodview__tanks') }}
)
,
units as (
    select
        *
    from {{ ref('stg_prodview__units') }}
)
select
    tank_record_id AS "Tank Record ID",
    --parent_record_id AS "Parent Record ID",
    --flow_network_id AS "Flow Network ID",
    v.tank_id AS "Tank ID",
    --tank_table AS "Tank Table",
    tank_date AS "Date",
    opening_total_volume_bbl AS "Opening Total Volume (bbl)",
    opening_oil_condensate_volume_bbl AS "Opening Oil/Condensate Volume (bbl)",
    opening_gas_equivalent_oil_cond_volume_mcf AS "Opening Gas Equivalent of Oil/Cond Volume (Mcf)",
    opening_water_volume_bbl AS "Opening Water Volume (bbl)",
    opening_sand_volume_bbl AS "Opening Sand Volume (bbl)",
    opening_bsw_total_pct AS "Opening BSW Total (%)",
    opening_sand_cut_total_pct AS "Opening Sand Cut Total (%)",
    closing_total_volume_bbl AS "Closing Total Volume (bbl)",
    closing_oil_condensate_volume_bbl AS "Closing Oil/Condensate Volume (bbl)",
    closing_gas_equivalent_oil_cond_volume_mcf AS "Closing Gas Equivalent of Oil/Cond Volume (Mcf)",
    closing_water_volume_bbl AS "Closing Water Volume (bbl)",
    closing_sand_volume_bbl AS "Closing Sand Volume (bbl)",
    closing_bsw_total_pct AS "Closing BSW Total (%)",
    closing_sand_cut_total_pct AS "Closing Sand Cut Total (%)",
    change_total_volume_bbl AS "Change in Total Volume (bbl)",
    change_oil_condensate_volume_bbl AS "Change in Oil/Condensate Volume (bbl)",
    change_gas_equivalent_oil_cond_volume_mcf AS "Change in Gas Equivalent of Oil/Cond Volume (Mcf)",
    change_water_volume_bbl AS "Change in Water Volume (bbl)",
    change_sand_volume_bbl AS "Change in Sand Volume (bbl)",
    current_facility_id AS "Current Facility ID",
    unit_id AS "Unit ID",
    "Unit Type"
    --HC_LIQUID_ANALYSIS_ID,
    --HC_LIQUID_ANALYSIS_TABLE,
from tankvol v
     left join tanks t
     on v.tank_id = t.tank_id
     right join units u
     on t.unit_id = u."Unit Record ID"
     where not "Unit ID" is null