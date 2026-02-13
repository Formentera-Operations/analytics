{{
    config(
        enabled=true,
        materialized='view'
    )
}}

with tankvol as (
    select *
    from {{ ref('stg_prodview__tank_daily_volumes') }}
    where coalesce(_fivetran_deleted, false) = false
),

tanks as (
    select *
    from {{ ref('stg_prodview__tanks') }}
),

units as (
    select *
    from {{ ref('stg_prodview__units') }}
)

select
    v.id_rec as "Tank Record ID",
    v.tank_id as "Tank ID",
    tank_date as "Date",
    opening_total_volume_bbl as "Opening Total Volume (bbl)",
    opening_oil_condensate_volume_bbl as "Opening Oil/Condensate Volume (bbl)",
    opening_gas_equivalent_oil_cond_volume_mcf as "Opening Gas Equivalent of Oil/Cond Volume (Mcf)",
    opening_water_volume_bbl as "Opening Water Volume (bbl)",
    opening_sand_volume_bbl as "Opening Sand Volume (bbl)",
    opening_bsw_total_pct as "Opening BSW Total (%)",
    opening_sand_cut_total_pct as "Opening Sand Cut Total (%)",
    closing_total_volume_bbl as "Closing Total Volume (bbl)",
    closing_oil_condensate_volume_bbl as "Closing Oil/Condensate Volume (bbl)",
    closing_gas_equivalent_oil_cond_volume_mcf as "Closing Gas Equivalent of Oil/Cond Volume (Mcf)",
    closing_water_volume_bbl as "Closing Water Volume (bbl)",
    closing_sand_volume_bbl as "Closing Sand Volume (bbl)",
    closing_bsw_total_pct as "Closing BSW Total (%)",
    closing_sand_cut_total_pct as "Closing Sand Cut Total (%)",
    change_total_volume_bbl as "Change in Total Volume (bbl)",
    change_oil_condensate_volume_bbl as "Change in Oil/Condensate Volume (bbl)",
    change_gas_equivalent_oil_cond_volume_mcf as "Change in Gas Equivalent of Oil/Cond Volume (Mcf)",
    change_water_volume_bbl as "Change in Water Volume (bbl)",
    change_sand_volume_bbl as "Change in Sand Volume (bbl)",
    current_facility_id as "Current Facility ID",
    t.id_rec_parent as "Unit ID",
    "Unit Type"
from units as u
left join tanks as t
    on u."Unit Record ID" = t.id_rec_parent
left join tankvol as v
    on t.id_rec = v.tank_id
where t.id_rec_parent is not null
