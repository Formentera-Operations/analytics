{{
    config(
        materialized='view'
    )
}}

with pvunit as (
    select * from {{ ref('stg_prodview__units') }}
),

pvunitcomp as (
    select * from {{ ref('stg_prodview__completions') }}
),

svintegration as (
    select *
    from {{ ref('stg_prodview__system_integrations') }}
    where
        product_description = 'SiteView'
        and table_key_parent = 'pvunit'
),

wvcompintegration as (
    select *
    from {{ ref('stg_prodview__system_integrations') }}
    where
        product_description = 'WellView'
        and table_key_parent = 'pvunitcomp'
),

wvintegration as (
    select *
    from {{ ref('stg_prodview__system_integrations') }}
    where
        product_description = 'WellView'
        and table_key_parent = 'pvunit'
)

select
    -- unit information
    u.api_10,
    u.asset_company,
    u.bha_change_1,
    u.bha_change_2,
    u.chemical_provider,
    u.combo_curve_id,
    u.commingle_permit_number,
    u.comments,
    u.company_code,
    u.completion_status,
    u.cost_center,
    u.country,
    u.current_route_id,
    u.created_at_utc as unit_created_at_utc,
    u.current_facility_id,
    u.dsu,
    u.district,
    u.district_office,
    u.electric_acct_number,
    u.electric_alloc_meter_number,
    u.electric_meter_id,
    u.electric_vendor_number,
    u.facility_name,
    u.field_office,
    u.id_flownet,
    u.foreman_area,
    u.gas_gathering_system_name,
    u.gas_purchaser_id,
    u.government_authority,
    u.ground_elevation_ft,
    u.hcliq_disposition_method,
    u.hcliq_inventory_type,
    u.hide_record_as_of,
    u.is_cycled,
    u.is_operated,
    u.is_purchaser,
    u.modified_by as unit_modified_by,
    u.modified_at_utc as unit_modified_at_utc,
    u.latlong_data_source,
    u.latlong_datum,
    u.lease_name,
    u.operated_descriptor,
    u.operator_name,
    u.pad_name,
    u.producing_method,
    u.property_eid,
    u.property_number,
    u.regulatory_field_name,
    u.regulatory_id,
    u.regulatory_unit_type,
    u.start_display_date,
    u.state_province,
    u.stop_display_date,
    u.stripper_date,
    u.stripper_type,
    u.surface_latitude,
    u.surface_legal_location,
    u.surface_longitude,
    u.swd_system,
    u.lease_number,
    u.utm_easting,
    u.utm_grid_zone,
    u.utm_northing,
    u.unit_display_name,
    u.unit_name,
    u.id_rec as unit_record_id,
    u.unit_sub_type,
    u.unit_type,

    -- completion information
    c.abandon_date,
    c.bha_type,
    c.bottomhole_latitude,
    c.bottomhole_longitude,
    c.completion_license_date,
    c.completion_licensee,
    c.completion_name,
    c.id_rec as completion_record_id,
    c.created_at_utc as completion_created_at_utc,
    c.created_by as completion_created_by,
    c.electric_meter_name,
    c.electric_vendor_name,
    c.entry_req_period_fluid_level,
    c.entry_req_period_param,
    c.expiry_date,
    c.export_id_1,
    c.export_id_2,
    c.export_type_1,
    c.export_type_2,
    c.federal_lease_number,
    c.first_sale_date,
    c.flowback_end_date,
    c.flowback_start_date,
    c.ghg_report_basin,
    c.gas_alloc_group_number,
    c.gas_alloc_meter_number,
    c.gas_meter_number,
    c.gas_pop_id,
    c.held_by_production_threshold,
    c.import_id_1,
    c.import_id_2,
    c.import_type_1,
    c.import_type_2,
    c.well_license_number,
    c.modified_by as completion_modified_by,
    c.modified_at_utc as completion_modified_at_utc,
    c.last_produced_date,
    c.last_produced_gas_date,
    c.last_produced_oil_date,
    c.legal_well_name,
    c.pop_date,
    c.prod_casing,
    c.prod_liner,
    c.producing_formation,
    c.completion_prod_acct_id,
    c.well_prod_acct_id,
    c.purchaser_ctb_lease_id,
    c.purchaser_well_lease_id,
    c.reserve_category,
    c.well_regulatory_id,
    c.rig_release_date,
    c.spud_date,
    c.start_allocating_date,
    c.surface_casing,
    c.surface_commingle_number,
    c.well_name,
    c.well_number,
    c.working_interest_partner,
    c.user_date_2,

    -- integration ids
    si.af_id_rec as siteview_site_id,
    wci.af_id_rec as wellview_completion_id,
    wi.af_id_rec as wellview_well_id,

    -- update tracking
    greatest(
        coalesce(u.modified_at_utc, '0000-01-01T00:00:00.000Z'),
        coalesce(c.modified_at_utc, '0000-01-01T00:00:00.000Z'),
        coalesce(si.modified_at_utc, '0000-01-01T00:00:00.000Z'),
        coalesce(wci.modified_at_utc, '0000-01-01T00:00:00.000Z'),
        coalesce(wi.modified_at_utc, '0000-01-01T00:00:00.000Z')
    ) as last_modified_at_utc

from pvunit as u
left join pvunitcomp as c
    on u.id_rec = c.id_rec_parent
left join svintegration as si
    on
        u.id_rec = si.id_rec_parent
        and u.id_flownet = si.id_flownet
left join wvcompintegration as wci
    on
        u.id_rec = wci.id_rec_parent
        and u.id_flownet = wci.id_flownet
left join wvintegration as wi
    on
        u.id_rec = wi.id_rec_parent
        and u.id_flownet = wi.id_flownet
