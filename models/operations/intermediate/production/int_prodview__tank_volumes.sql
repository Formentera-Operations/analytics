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
    v.id_rec as tank_record_id,
    v.tank_id,
    tank_date,
    opening_total_volume_bbl,
    opening_oil_condensate_volume_bbl,
    opening_gas_equivalent_oil_cond_volume_mcf,
    opening_water_volume_bbl,
    opening_sand_volume_bbl,
    opening_bsw_total_pct,
    opening_sand_cut_total_pct,
    closing_total_volume_bbl,
    closing_oil_condensate_volume_bbl,
    closing_gas_equivalent_oil_cond_volume_mcf,
    closing_water_volume_bbl,
    closing_sand_volume_bbl,
    closing_bsw_total_pct,
    closing_sand_cut_total_pct,
    change_total_volume_bbl,
    change_oil_condensate_volume_bbl,
    change_gas_equivalent_oil_cond_volume_mcf,
    change_water_volume_bbl,
    change_sand_volume_bbl,
    v.current_facility_id,
    t.id_rec_parent as unit_id,
    u.unit_type
from tankvol as v
inner join tanks as t
    on v.tank_id = t.id_rec
inner join units as u
    on t.id_rec_parent = u.id_rec
