{{
  config(
    materialized='view',
    tags=['wellview', 'intermediate', 'well-dimension']
  )
}}

with well_header as (
    select
        *
    from {{ ref('stg_wellview__well_header') }}
),

wellbores as (
    select
        wb.*,
        row_number() over (
            partition by wb.well_id
            order by
                coalesce(wb.current_status_date, wb.modified_at, wb.created_at) desc,
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
        {{ dbt_utils.generate_surrogate_key(['h."Well ID"']) }} as well_sk,
        {{ dbt_utils.generate_surrogate_key(['h."Well ID"', 'lw.record_id']) }} as well_bore_sk,
        h."Well ID" as well_id,
        h."Well Name" as well_name,
        h."API 10 Number" as api_10_number,
        h."Unique Well Identifier" as unique_well_identifier,
        h."Cost Center" as cost_center,
        h."Pad Name" as pad_name,
        --h."Pad Code" as facility_name,
        h."Lease Name" as lease_name,
        h."Asset Company" as asset_company,
        h."Company Code" as company_code,
        h."Field Name" as field_name,
        h."Regulatory Field Name" as regulatory_field_name,
        h."Basin Name" as basin_name,
        h."Well Type" as well_type,
        h."Well Subtype" as well_subtype,
        h."Current Well Type" as current_well_type,
        h."Current Well Subtype" as current_well_subtype,
        h."Current Well Status" as current_well_status,
        h."Current Well Sub Status" as current_well_sub_status,
        h."Current Status Date" as current_status_date,
        h."Fluid Type" as fluid_type,
        h."Current Fluid Type" as current_fluid_type,
        h."Well Configuration Type" as well_configuration_type,
        h."Number Of Wellbores" as number_of_wellbores,
        h."Onshore Offshore Designation" as onshore_offshore_designation,
        h."State Province" as state_province,
        h."County Parish" as county_parish,
        h."Latitude Degrees" as surface_latitude_degrees,
        h."Longitude Degrees" as surface_longitude_degrees,
        h."UTM Easting Meters" as surface_utm_easting_meters,
        h."UTM Northing Meters" as surface_utm_northing_meters,
        h."Spud Date" as spud_date,
        h."Rig Release Date" as rig_release_date,
        h."On Production Date" as on_production_date,
        h."Last Production Date" as last_production_date,
        h."Abandon Date" as abandon_date,
        h."Last Approved MIT Date" as last_approved_mit_date,
        h."Number Of Casing Strings" as number_of_casing_strings,
        h."Production Setting ID" as production_setting_id,
        h."Production Setting Table Key" as production_setting_table_key,
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
        on lw.well_id = h."Well ID"
    left join latest_status ls
        on ls.well_id = h."Well ID"
    left join prior_status ps
        on ps.well_id = h."Well ID"
)

select *
from final