{{
  config(
    materialized='view',
    tags=['wellview', 'intermediate', 'well-dimension']
  )
}}

with well_header as (
    select *
    from {{ ref('stg_wellview__well_header') }}
),

wellbores as (
    select
        wb.*,
        row_number() over (
            partition by wb.well_id
            order by
                coalesce(wb.current_status_date, wb.modified_at_utc, wb.created_at_utc) desc,
                wb.record_id desc
        ) as bore_rank
    from {{ ref('stg_wellview__wellbores') }} wb
),

latest_wellbore as (
    select *
    from wellbores
    where bore_rank = 1
),

status_history as (
    select
        sh.*,
        row_number() over (
            partition by sh.well_id
            order by
                sh.status_date desc,
                sh.record_id desc
        ) as status_rank,
        lead(sh.status_date) over (
            partition by sh.well_id
            order by
                sh.status_date desc,
                sh.record_id desc
        ) as next_status_date
    from {{ ref('stg_wellview__well_status_history') }} sh
),

latest_status as (
    select *
    from status_history
    where status_rank = 1
),

prior_status as (
    select *
    from status_history
    where status_rank = 2
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['h.well_id']) }} as well_sk,
        {{ dbt_utils.generate_surrogate_key(['h.well_id', 'lw.record_id']) }} as well_bore_sk,
        h.well_id,
        h.well_name,
        h.api_10_number,
        h.unique_well_identifier,
        h.cost_center,
        h.pad_name,
        h.lease_name,
        h.asset_company,
        h.company_code,
        h.field_name,
        h.regulatory_field_name,
        h.basin_name,
        h.well_type,
        h.well_subtype,
        h.current_well_type,
        h.current_well_subtype,
        h.current_well_status,
        h.current_well_sub_status,
        h.current_status_date,
        h.fluid_type,
        h.current_fluid_type,
        h.well_configuration_type,
        h.number_of_wellbores,
        h.onshore_offshore_designation,
        h.state_province,
        h.county_parish,
        h.latitude_degrees as surface_latitude_degrees,
        h.longitude_degrees as surface_longitude_degrees,
        h.utm_easting_meters as surface_utm_easting_meters,
        h.utm_northing_meters as surface_utm_northing_meters,
        h.spud_date,
        h.rig_release_date,
        h.on_production_date,
        h.last_production_date,
        h.abandon_date,
        h.last_approved_mit_date,
        h.number_of_casing_strings,
        h.production_setting_id,
        h.production_setting_table_key,
        lw.record_id as wellbore_record_id,
        lw.wellbore_name,
        lw.wellbore_api_uwi,
        lw.purpose as wellbore_purpose,
        lw.profile_type as wellbore_profile_type,
        lw.current_status as wellbore_current_status,
        lw.current_status_date as wellbore_current_status_date,
        lw.start_drill_date as wellbore_start_drill_date,
        lw.end_drill_date as wellbore_end_drill_date,
        lw.total_depth_ft as wellbore_total_depth_ft,
        lw.total_depth_tvd_ft as wellbore_total_depth_tvd_ft,
        lw.max_inclination_degrees as wellbore_max_inclination_degrees,
        lw.max_dls_degrees_per_100ft as wellbore_max_dls_deg_per_100ft,
        lw.max_departure_ft as wellbore_max_departure_ft,
        lw.latitude_degrees as wellbore_latitude_degrees,
        lw.longitude_degrees as wellbore_longitude_degrees,
        lw.utm_easting_meters as wellbore_utm_easting_meters,
        lw.utm_northing_meters as wellbore_utm_northing_meters,
        lw.location_note as wellbore_location_note,
        lw.bottom_hole_legal_location,
        ls.status_date as latest_status_date,
        ls.well_status as latest_well_status,
        ls.well_sub_status as latest_well_sub_status,
        ls.well_type as latest_well_type,
        ls.well_subtype as latest_well_subtype,
        ls.primary_fluid_type as latest_primary_fluid_type,
        ls.status_source as latest_status_source,
        ps.status_date as prior_status_date,
        ps.well_status as prior_well_status,
        ps.well_sub_status as prior_well_sub_status,
        ps.well_type as prior_well_type,
        ps.well_subtype as prior_well_subtype,
        ps.primary_fluid_type as prior_primary_fluid_type,
        ps.status_source as prior_status_source,
        ls.status_rank = 1 as is_current_status_available,
        ps.status_rank = 2 as is_prior_status_available,
        current_timestamp as dim_created_at,
        current_timestamp as dim_updated_at
    from well_header h
    left join latest_wellbore lw
        on h.well_id = lw.well_id
    left join latest_status ls
        on h.well_id = ls.well_id
    left join prior_status ps
        on h.well_id = ps.well_id
)

select *
from final
