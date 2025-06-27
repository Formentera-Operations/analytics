{{
  config(
    materialized='view',
    alias='unit_v2'
  )
}}

with pvunit as (
    select * from {{ ref('stg_prodview__pvunit') }}
    where deleted = false
),

pvroutesetroute as (
    select * from {{ ref('stg_prodview__pvroutesetroute') }}
    where deleted = false
),

pvunitcomp as (
    select * from {{ ref('stg_prodview__pvunitcomp') }}
    where deleted = false
),

pvsysintegration as (
    select * from {{ ref('stg_prodview__pvsysintegration') }}
    where deleted = false
),

wvsysintegration as (
    select * from {{ ref('stg_wellview__wvsysintegration') }}
    where deleted = false
)

-- TODO: Add siteview integration CTE when data becomes available:
-- svsysintegration as (
--     select * from ('stg_siteview__svsysintegration')
--     where deleted = false
-- )

select
    -- Route information
    pvroutesetroute.name as route_name,
    pvroutesetroute.user_txt_1 as route_user_txt_1,
    pvroutesetroute.user_txt_2 as route_user_txt_2,
    pvroutesetroute.user_txt_3 as route_user_txt_3,
    
    -- Unit geographic and administrative information
    pvunit.area,
    pvunit.com,
    pvunit.cost_center_id_a,
    pvunit.cost_center_id_b,
    pvunit.country,
    pvunit.county,
    pvunit.disp_product_name,
    pvunit.district,
    pvunit.division,
    pvunit.division_code,
    pvunit.dttm_end,
    pvunit.dttm_start,
    pvunit.dttm_hide,
    pvunit.elevation,
    pvunit.id_rec_facility_calc as facility_id,
    pvunit.field,
    pvunit.field_code,
    pvunit.field_office,
    pvunit.field_office_code,
    pvunit.id_flow_net as flow_net_id,
    pvunit.latitude,
    pvunit.lease,
    pvunit.lease_id_a,
    pvunit.legal_surf_loc,
    pvunit.location_type,
    pvunit.longitude,
    pvunit.lat_long_source,
    pvunit.lat_long_datum,
    pvunit.utm_grid_zone,
    pvunit.utm_source,
    pvunit.utm_x,
    pvunit.utm_y,
    pvunit.name,
    pvunit.name_short,
    pvunit.operated,
    pvunit.operator_id_a,
    pvunit.operator,
    pvunit.pad_code,
    pvunit.pad_name,
    pvunit.platform,
    pvunit.slot,
    pvunit.gov_authority,
    pvunit.id_rec_resp_1 as primary_resp_team_id,
    pvunit.id_rec_resp_2 as secondary_resp_team_id,
    pvunit.purchaser,
    pvunit.priority,
    pvunit.state_prov,
    pvunit.sys_create_date,
    pvunit.sys_create_user,
    pvunit.sys_mod_date,
    pvunit.sys_mod_user,
    pvunit.type_1,
    pvunit.type_2,
    pvunit.type_disp_hc_liq,
    pvunit.type_pa,
    pvunit.type_regulatory,
    pvunit.id_rec as unit_id,
    pvunit.unit_id_a,
    pvunit.unit_id_b,
    pvunit.unit_id_c,
    pvunit.unit_id_pa,
    pvunit.unit_id_regulatory,
    pvunit.key_migration_source,
    pvunit.type_migration_source,
    pvunit.user_num_1,
    pvunit.user_num_2,
    pvunit.user_num_3,
    pvunit.user_num_4,
    pvunit.user_num_5,
    pvunit.user_txt_1,
    pvunit.user_txt_2,
    pvunit.user_txt_3,
    pvunit.user_txt_4,
    pvunit.user_txt_5,
    pvunit.user_dttm_1,
    pvunit.user_dttm_2,
    pvunit.user_dttm_3,
    pvunit.user_dttm_4,
    pvunit.user_dttm_5,
    
    -- Completion information
    pvunitcomp.alloc_start_date,
    pvunitcomp.comp_dttm_end,
    pvunitcomp.dttm_last_produced_calc,
    pvunitcomp.dttm_last_produced_hc_liq_calc,
    pvunitcomp.dttm_last_produced_gas_calc,
    pvunitcomp.held_by_production_threshold,
    pvunitcomp.comp_id_a,
    pvunitcomp.comp_id_b,
    pvunitcomp.comp_id_c,
    pvunitcomp.comp_id_d,
    pvunitcomp.comp_id_e,
    pvunitcomp.comp_id_pa,
    pvunitcomp.completion_code,
    pvunitcomp.comp_id_regulatory,
    pvunitcomp.completion_name,
    pvunitcomp.completion_licensee,
    pvunitcomp.completion_license_no,
    pvunitcomp.comp_sys_create_date,
    pvunitcomp.comp_sys_create_user,
    pvunitcomp.comp_sys_mod_date,
    pvunitcomp.comp_sys_mod_user,
    pvunitcomp.comp_user_dttm_1,
    pvunitcomp.comp_user_dttm_2,
    pvunitcomp.comp_user_dttm_3,
    pvunitcomp.comp_user_dttm_4,
    pvunitcomp.comp_user_dttm_5,
    pvunitcomp.comp_user_num_1,
    pvunitcomp.comp_user_num_2,
    pvunitcomp.comp_user_num_3,
    pvunitcomp.comp_user_num_4,
    pvunitcomp.comp_user_num_5,
    pvunitcomp.comp_user_txt_1,
    pvunitcomp.comp_user_txt_2,
    pvunitcomp.comp_user_txt_3,
    pvunitcomp.comp_user_txt_4,
    pvunitcomp.comp_user_txt_5,
    pvunitcomp.comp_latitude,
    pvunitcomp.comp_longitude,
    pvunitcomp.comp_lat_long_source,
    pvunitcomp.comp_lat_long_datum,
    pvunitcomp.entry_req_period_fluid_level,
    pvunitcomp.entry_req_period_param,
    pvunitcomp.dttm_license,
    pvunitcomp.export_id_1,
    pvunitcomp.export_id_2,
    pvunitcomp.export_type_1,
    pvunitcomp.export_type_2,
    pvunitcomp.first_sale_date,
    pvunitcomp.flowback_end_date,
    pvunitcomp.flowback_start_date,
    pvunitcomp.abandon_date,
    pvunitcomp.comp_key_migration_source,
    pvunitcomp.comp_type_migration_source,
    pvunitcomp.import_id_1,
    pvunitcomp.import_id_2,
    pvunitcomp.import_type_1,
    pvunitcomp.import_type_2,
    pvunitcomp.production_date,
    pvunitcomp.well_id_a,
    pvunitcomp.well_id_b,
    pvunitcomp.well_id_c,
    pvunitcomp.well_id_d,
    pvunitcomp.well_id_e,
    pvunitcomp.well_id_pa,
    pvunitcomp.well_id_regulatory,
    pvunitcomp.well_license_no,
    pvunitcomp.well_name,
    
    -- TODO: SiteView integration - uncomment when data becomes available
    -- svintegration.af_id_rec as sv_site_id,
    null as sv_site_id,
    
    -- WellView integrations
    wvcompintegration.af_id_rec as wv_completion_id,
    wvintegration.af_id_rec as wv_well_id,
    
    -- Update date (maximum of all joined tables)
    greatest(
        coalesce(pvroutesetroute.update_date, '0000-01-01T00:00:00.000Z'::timestamp_tz),
        coalesce(pvunit.update_date, '0000-01-01T00:00:00.000Z'::timestamp_tz),
        coalesce(pvunitcomp.update_date, '0000-01-01T00:00:00.000Z'::timestamp_tz),
        coalesce(wvcompintegration.update_date, '0000-01-01T00:00:00.000Z'::timestamp_tz),
        coalesce(wvintegration.update_date, '0000-01-01T00:00:00.000Z'::timestamp_tz)
        -- TODO: Add SiteView update date when available
        -- coalesce(svintegration.update_date, '0000-01-01T00:00:00.000Z'::timestamp_tz)
    ) as update_date

from pvunit
left join pvroutesetroute 
    on pvunit.id_rec_routeset_route_calc = pvroutesetroute.id_rec
left join pvunitcomp 
    on pvunit.id_rec = pvunitcomp.id_rec_parent
-- TODO: Add SiteView integration join when data becomes available:
-- left join svsysintegration as svintegration 
--     on pvunit.id_rec = svintegration.id_rec_parent 
--     and svintegration.af_product = 'SiteView' 
--     and svintegration.tbl_key_parent = 'pvunit' 
--     and svintegration.id_flow_net = pvunit.id_flow_net
left join pvsysintegration as wvcompintegration 
    on pvunit.id_rec = wvcompintegration.id_rec_parent 
    and wvcompintegration.af_product = 'WellView' 
    and wvcompintegration.tbl_key_parent = 'pvunitcomp' 
    and wvcompintegration.id_flow_net = pvunit.id_flow_net
left join pvsysintegration as wvintegration 
    on pvunit.id_rec = wvintegration.id_rec_parent 
    and wvintegration.af_product = 'WellView' 
    and wvintegration.tbl_key_parent = 'pvunit' 
    and wvintegration.id_flow_net = pvunit.id_flow_net