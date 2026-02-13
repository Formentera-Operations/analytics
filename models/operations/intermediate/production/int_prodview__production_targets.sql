{{
    config(
        enabled=true,
        materialized='view'
    )
}}

with targetdaily as (
    select *
    from {{ ref('stg_prodview__production_targets_daily') }}
),

parenttarget as (
    select *
    from {{ ref('stg_prodview__production_targets') }}
),

header as (
    select *
    from {{ ref('int_prodview__well_header') }}
),

joined as (
    select
        p.cc_forecast_name,
        t.created_at_utc,
        t.created_by,
        t.id_flownet,
        p.is_use_in_diff_from_target_calculations,
        t.modified_at_utc,
        t.modified_by,
        t.target_daily_date as prod_date,
        t.target_daily_rate_condensate_bbl_per_day,
        t.target_daily_rate_gas_mcf_per_day,
        t.target_daily_rate_hcliq_bbl_per_day,
        t.target_daily_rate_ngl_bbl_per_day,
        t.target_daily_rate_oil_bbl_per_day,
        t.target_daily_rate_sand_bbl_per_day,
        t.target_daily_rate_water_bbl_per_day,
        t.id_rec as target_daily_record_id,
        p.id_rec as target_record_id,
        p.target_start_date,
        p.target_type,
        h.unit_record_id
    from targetdaily as t
    left join parenttarget as p
        on t.id_rec_parent = p.id_rec
    left join header as h
        on p.id_rec_parent = h.completion_record_id
)

select *
from joined
