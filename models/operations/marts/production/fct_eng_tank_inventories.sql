{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'facts']
) }}


select
    tank_record_id as "Tank Record ID",
    tank_id as "Tank ID",
    tank_date as "Date",
    opening_total_volume_bbl as "Opening Total Volume BBL",
    opening_oil_condensate_volume_bbl as "Opening Oil Condensate Volume BBL",
    opening_gas_equivalent_oil_cond_volume_mcf as "Opening Gas Equivalent Oil Cond Volume MCF",
    opening_water_volume_bbl as "Opening Water Volume BBL",
    opening_sand_volume_bbl as "Opening Sand Volume BBL",
    opening_bsw_total_pct as "Opening BSW Total Pct",
    opening_sand_cut_total_pct as "Opening Sand Cut Total Pct",
    closing_total_volume_bbl as "Closing Total Volume BBL",
    closing_oil_condensate_volume_bbl as "Closing Oil Condensate Volume BBL",
    closing_gas_equivalent_oil_cond_volume_mcf as "Closing Gas Equivalent Oil Cond Volume MCF",
    closing_water_volume_bbl as "Closing Water Volume BBL",
    closing_sand_volume_bbl as "Closing Sand Volume BBL",
    closing_bsw_total_pct as "Closing BSW Total Pct",
    closing_sand_cut_total_pct as "Closing Sand Cut Total Pct",
    change_total_volume_bbl as "Change Total Volume BBL",
    change_oil_condensate_volume_bbl as "Change Oil Condensate Volume BBL",
    change_gas_equivalent_oil_cond_volume_mcf as "Change Gas Equivalent Oil Cond Volume MCF",
    change_water_volume_bbl as "Change Water Volume BBL",
    change_sand_volume_bbl as "Change Sand Volume BBL",
    current_facility_id as "Current Facility ID",
    unit_id as "Unit ID",
    unit_type as "Unit Type"
from {{ ref('int_prodview__tank_volumes') }}
where
    tank_date > last_day(dateadd(year, -3, current_date()), year)
    and current_facility_id is not null
