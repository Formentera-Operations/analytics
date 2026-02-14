{{
    config(
        enabled=true,
        materialized='view'
    )
}}

with header as (
    select
        -- identifiers
        well_id,
        well_name,
        api_10_number,
        cost_center,
        eid,
        asset_company,
        company_code,

        -- dates
        spud_date,
        permit_date,
        rig_release_date,
        on_production_date,
        abandon_date,
        first_sales_date,
        ops_effective_date,
        regulatory_effective_date,
        last_approved_mit_date,

        -- coordinates
        latitude_degrees,
        longitude_degrees,
        lat_long_datum,
        utm_easting_meters,
        utm_northing_meters,

        -- system / audit
        master_lock_date,
        last_write_to_database,
        system_lock_date,
        last_mod_at_utc
    from {{ ref('stg_wellview__well_header') }}
),

wvintegration as (
    select
        wi.af_id_rec,
        u.id_rec as unit_record_id
    from {{ ref('stg_prodview__system_integrations') }} as wi
    left join {{ ref('stg_prodview__units') }} as u
        on
            wi.id_rec_parent = u.id_rec
            and wi.id_flownet = u.id_flownet
    where
        wi.product_description = 'WellView'
        and wi.table_key_parent = 'pvunit'
),

source as (
    select
        h.well_id,
        h.well_name,
        h.api_10_number,
        h.cost_center,
        h.eid,
        h.asset_company,
        h.company_code,
        h.spud_date,
        h.permit_date,
        h.rig_release_date,
        h.on_production_date,
        h.abandon_date,
        h.first_sales_date,
        h.ops_effective_date,
        h.regulatory_effective_date,
        h.last_approved_mit_date,
        h.latitude_degrees,
        h.longitude_degrees,
        h.lat_long_datum,
        h.utm_easting_meters,
        h.utm_northing_meters,
        h.master_lock_date,
        h.last_write_to_database,
        h.system_lock_date,
        h.last_mod_at_utc,
        wi.unit_record_id,
        greatest(
            coalesce(h.last_mod_at_utc, '0000-01-01T00:00:00.000Z')
        ) as updated_date
    from header as h
    left join wvintegration as wi
        on h.well_id = wi.af_id_rec
)

select *
from source
order by well_id desc
