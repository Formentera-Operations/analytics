{{ config(
    materialized='view',
    tags=['wellview', 'rod', 'components', 'artificial_lift', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVRODCOMP') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as rod_component_id,
        idrecparent as rod_string_id,
        idwell as well_id,
        sysseq as sequence_number,
        
        -- Component description
        des as component_description,
        desjtcalc as description_with_joint_count,
        itemnocalc as item_number,
        iconname as icon_name,
        compsubtyp as equipment_type,
        
        -- Basic specifications (converted to US units)
        szodnom / 0.0254 as nominal_od_in,
        szidnom / 0.0254 as nominal_id_in,
        szodmax / 0.0254 as maximum_od_in,
        wtperlength / 1.48816394356955 as weight_per_length_lb_per_ft,
        grade as grade,
        length / 0.3048 as length_ft,
        joints as joint_count,
        
        -- Depth calculations (converted to US units)
        depthtopcalc / 0.3048 as top_depth_ft,
        depthbtmcalc / 0.3048 as bottom_depth_ft,
        depthtvdtopcalc / 0.3048 as top_depth_tvd_ft,
        depthtvdbtmcalc / 0.3048 as bottom_depth_tvd_ft,
        depthtopcorrected / 0.3048 as corrected_top_depth_ft,
        lengthcumcalc / 0.3048 as cumulative_length_ft,
        
        -- Connection specifications (converted to US units)
        conntyptop as top_connection_type,
        connthrdtop as top_connection_thread,
        connsztop / 0.0254 as top_connection_size_in,
        upsettop as top_upset,
        
        conntypbtm as bottom_connection_type,
        connthrdbtm as bottom_connection_thread,
        connszbtm / 0.0254 as bottom_connection_size_in,
        upsetbtm as bottom_upset,
        
        connectcalc as connections_description,
        connectaltcalc as connections_alt_description,
        
        -- Weight and volume calculations (converted to US units)
        weightcalc / 4.4482216152605 as component_weight_lbf,
        weightcumcalc / 4448.2216152605 as cumulative_weight_klbf,
        volumedispcalc / 0.158987294928 as volume_displaced_bbl,
        volumedispcumcalc / 0.158987294928 as cumulative_volume_displaced_bbl,
        
        -- Fishing specifications (converted to US units)
        fishneckod / 0.0254 as fishing_neck_od_in,
        fishnecklength / 0.3048 as fishing_neck_length_ft,
        
        -- Inclination calculations
        incltopcalc as top_inclination_deg,
        inclbtmcalc as bottom_inclination_deg,
        inclmaxcalc as max_inclination_deg,
        
        -- Scraper and guide information (converted to US units)
        guidedes as scraper_description,
        guidesperrod as scrapers_per_rod,
        guidesz / 0.0254 as scraper_size_in,
        guidematerial as scraper_material,
        
        -- Component details
        make as manufacturer,
        model as model,
        sn as serial_number,
        material as material,
        coating as coating,
        refid as reference_id,
        
        -- Manufacturing and operational info
        dttmmanufacture as manufacture_date,
        hoursstart / 0.0416666666666667 as starting_hours_hr,
        usedclass as condition_class,
        conditionrun as condition_when_run,
        conditionpull as condition_when_pulled,
        
        -- Equipment ratings (converted to US units)
        tensilemax / 4448.2216152605 as max_tensile_strength_klbf,
        
        -- Cost information
        cost as item_cost,
        costunitlabel as cost_unit_label,
        
        -- Comments
        com as comments,
        
        -- Related entities
        idreclastfailurecalc as last_failure_id,
        idreclastfailurecalctk as last_failure_table_key,
        
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