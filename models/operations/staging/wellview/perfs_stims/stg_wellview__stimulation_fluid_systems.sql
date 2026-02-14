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
        ph as ph_level,
        presvapor as vapor_pressure_kpa,
        environmenttyp as environment_type,  -- Note: Kept in kPa as per view
        evalmethod as evaluation_method,

        -- Environmental classification
        ratiovolumedesigncalc as volume_design_ratio_bbl_per_bbl,
        usernum1 as user_number_1,

        -- Physical specifications
        usertxt1 as user_text_1,
        com as comment,

        -- Volume information (converted to barrels)
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,

        -- User fields
        syslockdate as system_lock_date,
        syscreatedate as created_at,

        -- Comments
        syscreateuser as created_by,

        -- System locking fields
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,
        _fivetran_synced as fivetran_synced_at,
        viscosity / 0.001 as viscosity_cp,

        -- System tracking fields
        tempref / 0.555555555555556 + 32 as reference_temperature_fahrenheit,
        filtersz / 0.0254 as filter_size_inches,
        masstotal / 0.45359237 as mass_total_lb,
        volume / 0.158987294928 as volume_bbl,
        volumecalc / 0.158987294928 as volume_calc_bbl,

        -- Fivetran metadata
        volumedesign / 0.158987294928 as volume_design_bbl

    from source_data
)

select * from renamed
