{{ config(
    materialized='view',
    tags=['wellview', 'zones', 'production', 'intervals', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVZONE') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idwell as well_id,
        idrec as record_id,
        
        -- Zone identification
        zonename as zone_name,
        zonecode as zone_code,
        zoneida as zone_api_number,
        zoneidb as zone_id_b,
        zoneidc as zone_id_c,
        zoneidd as zone_id_d,
        zoneide as zone_id_e,
        
        -- Wellbore relationship
        idrecwellbore as wellbore_id,
        idrecwellboretk as wellbore_table_key,
        
        -- Zone depths (converted to US units)
        depthtop / 0.3048 as top_depth_ft,
        depthbtm / 0.3048 as bottom_depth_ft,
        depthref / 0.3048 as reference_depth_ft,
        depthtvdtopcalc / 0.3048 as top_depth_tvd_ft,
        depthtvdbtmcalc / 0.3048 as bottom_depth_tvd_ft,
        depthtvdrefcalc / 0.3048 as reference_depth_tvd_ft,
        depthtoptobtmcalc / 0.3048 as zone_thickness_ft,
        
        -- Zone characteristics
        objective as objective,
        formationcalc as formation,
        formationlayercalc as formation_layer,
        reservoircalc as reservoir,
        iconname as icon_name,
        
        -- License information
        zonelicenseno as zone_license_number,
        zonelicensee as zone_licensee,
        dttmzonelic as zone_license_date,
        
        -- Production dates
        dttmzoneonprodest as estimated_on_production_date,
        dttmzoneonprod as first_production_date,
        dttmzonelastprodest as estimated_last_production_date,
        dttmzonelastprod as last_production_date,
        dttmzoneabandonest as estimated_abandonment_date,
        dttmzoneabandon as abandon_date,
        
        -- Current status
        currentstatuscalc as current_status,
        dttmstatuscalc as current_status_date,
        
        -- Field information
        fieldname as field_name,
        fieldcode as field_code,
        
        -- Unit information
        unitname as unit_name,
        unitcode as unit_code,
        
        -- Completion reference
        idreclastcompletioncalc as last_completion_id,
        idreclastcompletioncalctk as last_completion_table_key,
        
        -- Data source
        datasource as data_source,
        
        -- User fields
        usertxt1 as user_text_1,
        usertxt2 as user_text_2,
        usertxt3 as user_text_3,
        usertxt4 as user_text_4,
        usertxt5 as user_text_5,
        usertxt6 as user_text_6,
        
        -- Comments
        com as comment,

        -- System locking fields
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockdate as system_lock_date,

        -- System tracking fields
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,

        -- Fivetran metadata
        _fivetran_synced as fivetran_synced_at

    from source_data
)

select * from renamed