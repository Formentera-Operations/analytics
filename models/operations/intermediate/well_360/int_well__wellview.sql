{{
    config(
        materialized='view',
        tags=['well_360', 'source_prep']
    )
}}

{#
    WellView Well Attributes
    ========================
    Prepares WellView data for Well 360 integration.
    
    Source Priority: Primary for drilling ops, spud dates, permit dates, configuration
    Deduplication: Uses most recent well_id per EID
    
    Note: WellView staging uses quoted column names with spaces
#}

with source as (
    select 
        "EID" as eid,
        "Well ID" as well_id,
        "Well Name" as well_name,
        "API 10 Number" as api_10,
        "Cost Center" as cost_center,
        "Is Operated" as is_operated,
        "Operated Descriptor" as operated_descriptor,
        "Operator Name" as operator_name,
        "Asset Company" as asset_company,
        "Company Code" as company_code,
        "Division" as division,
        "Spud Date" as spud_date,
        "Permit Date" as permit_date,
        "Basin Name" as basin_name,
        "Field Name" as field_name,
        "District" as district,
        "State Province" as state_province,
        "County Parish" as county_parish,
        "Country" as country,
        "Regulatory Field Name" as regulatory_field_name,
        "Field Office" as field_office,
        "Latitude Degrees" as latitude_degrees,
        "Longitude Degrees" as longitude_degrees,
        "Lat/Long Datum" as lat_long_datum,
        "Well Configuration Type" as well_configuration_type,
        "Unwrapped Displacement ft" as unwrapped_displacement_ft,
        "Rig Release Date" as rig_release_date,
        "On Production Date" as on_production_date,
        "Total Depth ft" as total_depth_ft,
        "Current Well Status" as current_well_status,
        "Lease Name" as lease_name,
        "Pad Name" as pad_name,
        "Route" as route,
        "Working Interest" as working_interest,
        "NRI Total" as nri_total
    from {{ ref('stg_wellview__well_header') }}
    where 
        len("EID") = 6
        and "EID" is not null
),

deduplicated as (
    select *
    from source
    qualify row_number() over (
        partition by eid 
        order by 
            -- Prefer records with more complete data
            case when spud_date is not null then 0 else 1 end,
            case when api_10 is not null then 0 else 1 end,
            well_id desc  -- Most recent as tiebreaker
    ) = 1
)

select * from deduplicated