{{ config(
    materialized='view',
    tags=['wellview', 'wellhead', 'components', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVWELLHEADCOMP') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as wellhead_component_id,
        idwell as well_id,
        idrecparent as wellhead_id,
        
        -- String relationships
        idrecstring as string_id,
        idrecstringtk as string_table_key,
        
        -- Component classification
        typ1 as component_type,
        typ2 as component_subtype,
        des as component_description,
        sect as component_section,
        
        -- Manufacturer information
        make as manufacturer,
        model as component_model,
        sn as serial_number,
        material as component_material,
        refid as reference_id,
        
        -- Physical dimensions (converted to US units)
        szid / 0.0254 as inner_diameter_in,
        szidnom / 0.0254 as nominal_inner_diameter_in,
        szodnom / 0.0254 as nominal_outer_diameter_in,
        length / 0.3048 as component_length_ft,
        lengthcumcalc / 0.3048 as cumulative_length_ft,
        minbore / 0.0254 as minimum_bore_in,
        
        -- Calculated depths (converted to US units)
        depthtopcalc / 0.3048 as top_depth_ft,
        depthbtmcalc / 0.3048 as bottom_depth_ft,
        
        -- Pressure ratings (converted to US units)
        workpres / 6.894757 as working_pressure_psi,
        maxpres / 6.894757 as maximum_pressure_psi,
        workprestop / 6.894757 as top_working_pressure_psi,
        workpresbtm / 6.894757 as bottom_working_pressure_psi,
        
        -- Temperature rating (converted to US units)
        case 
            when temprating is not null 
            then temprating / 0.555555555555556 + 32 
            else null 
        end as temperature_rating_f,
        
        -- Connection specifications (converted to US units)
        conntoptyp as top_connection_type,
        conntopsz / 0.0254 as top_connection_size_in,
        connbtmtyp as bottom_connection_type,
        connbtmsz / 0.0254 as bottom_connection_size_in,
        
        -- Volume and capacity (converted to US units)
        volumevoid / 0.158987294928 as void_volume_bbl,
        
        -- Service and specification details
        service as service_type,
        productspeclevel as product_specification_level,
        packofftype as packoff_type,
        
        -- Dates
        dttmstart as installation_datetime,
        dttmend as removal_datetime,
        dttmmanufacture as manufacture_datetime,
        
        -- Cost information
        cost as component_cost,
        costunitlabel as cost_unit_label,
        
        -- Visual and user information
        iconname as icon_name,
        usertxt as user_text,
        com as comments,
        
        -- Calculated relationships
        idreclastfailurecalc as last_failure_id,
        idreclastfailurecalctk as last_failure_table_key,
        
        -- Metric equivalents for reference
        szid as inner_diameter_m,
        szidnom as nominal_inner_diameter_m,
        szodnom as nominal_outer_diameter_m,
        length as component_length_m,
        lengthcumcalc as cumulative_length_m,
        minbore as minimum_bore_m,
        depthtopcalc as top_depth_m,
        depthbtmcalc as bottom_depth_m,
        workpres as working_pressure_kpa,
        maxpres as maximum_pressure_kpa,
        workprestop as top_working_pressure_kpa,
        workpresbtm as bottom_working_pressure_kpa,
        temprating as temperature_rating_c,
        conntopsz as top_connection_size_m,
        connbtmsz as bottom_connection_size_m,
        volumevoid as void_volume_m3,
        
        -- System fields
        sysseq as sequence_number,
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