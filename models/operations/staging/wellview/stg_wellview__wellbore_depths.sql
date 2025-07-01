{{ config(
    materialized='view',
    tags=['wellview', 'wellbore', 'key_depths', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVWELLBOREKEYDEPTH') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as key_depth_id,
        idwell as well_id,
        idrecparent as wellbore_id,
        
        -- Key depth classification
        proposedoractual as proposed_or_actual,
        keydepthtyp as key_depth_type,
        typ1 as key_depth_category,
        typ2 as key_depth_subcategory,
        des as key_depth_description,
        method as measurement_method,
        
        -- Timing
        dttm as key_depth_datetime,
        
        -- Depths (converted to US units)
        depthtop / 0.3048 as top_depth_ft,
        depthbtm / 0.3048 as bottom_depth_ft,
        depthtvdtopcalc / 0.3048 as top_depth_tvd_ft,
        depthtvdbtmcalc / 0.3048 as bottom_depth_tvd_ft,
        lengthcalc / 0.3048 as interval_length_ft,
        
        -- Location coordinates
        latitude as latitude_degrees,
        longitude as longitude_degrees,
        latlongsource as lat_long_data_source,
        
        -- UTM coordinates (kept in meters per view definition)
        utmx as utm_easting_m,
        utmy as utm_northing_m,
        utmgridzone as utm_grid_zone,
        utmsource as utm_data_source,
        
        -- Control flags
        case when exclude = 1 then true else false end as exclude_from_cost_calculations,
        
        -- Comments
        com as comments,
        
        -- System fields
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,
        syslockdate as system_lock_date,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        
        -- Fivetran fields
        _fivetran_synced as fivetran_synced_at
        
    from source_data
)

select * from renamed