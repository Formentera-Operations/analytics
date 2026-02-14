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

    Note: WellView staging now uses snake_case column names
#}

with source as (
    select
        eid,
        well_id,
        well_name,
        api_10_number as api_10,
        cost_center,
        is_operated,
        operated_descriptor,
        operator_name,
        asset_company,
        company_code,
        division,
        spud_date,
        permit_date,
        basin_name,
        field_name,
        district,
        state_province,
        county_parish,
        country,
        regulatory_field_name,
        field_office,
        latitude_degrees,
        longitude_degrees,
        lat_long_datum,
        well_configuration_type,
        unwrapped_displacement_ft,
        rig_release_date,
        on_production_date,
        total_depth_ft,
        current_well_status,
        lease_name,
        pad_name,
        route,
        working_interest,
        nri_total
    from {{ ref('stg_wellview__well_header') }}
    where
        len(eid) = 6
        and eid is not null
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
