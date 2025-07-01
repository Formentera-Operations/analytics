{{ config(
    materialized='view',
    tags=['wellview', 'stimulation', 'fluids', 'systems', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVSTIMINTFLUID') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idwell as well_id,
        idrecparent as parent_record_id,
        idrec as record_id,
        
        -- Fluid identification
        des as fluid_description,
        fluidname as fluid_name,
        vendorfluidname as vendor_fluid_name,
        typ1 as fluid_type_1,
        typ2 as fluid_type_2,
        purpose as purpose,
        
        -- Vendor information
        vendor as vendor,
        vendorcode as vendor_code,
        source as source,
        
        -- Fluid properties
        fluiddensity as fluid_density_api,  -- Note: Complex conversion applied in view
        viscosity / 0.001 as viscosity_cp,
        ph as ph_level,
        presvapor as vapor_pressure_kpa,  -- Note: Kept in kPa as per view
        tempref / 0.555555555555556 + 32 as reference_temperature_fahrenheit,
        
        -- Environmental classification
        environmenttyp as environment_type,
        evalmethod as evaluation_method,
        
        -- Physical specifications
        filtersz / 0.0254 as filter_size_inches,
        masstotal / 0.45359237 as mass_total_lb,
        
        -- Volume information (converted to barrels)
        volume / 0.158987294928 as volume_bbl,
        volumecalc / 0.158987294928 as volume_calc_bbl,
        volumedesign / 0.158987294928 as volume_design_bbl,
        ratiovolumedesigncalc as volume_design_ratio_bbl_per_bbl,
        
        -- User fields
        usernum1 as user_number_1,
        usertxt1 as user_text_1,
        
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