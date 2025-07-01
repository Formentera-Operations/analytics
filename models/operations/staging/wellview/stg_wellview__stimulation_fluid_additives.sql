{{ config(
    materialized='view',
    tags=['wellview', 'stimulation', 'fluid-additives', 'chemicals', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVSTIMINTFLUIDADD') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idwell as well_id,
        idrecparent as parent_record_id,
        idrec as record_id,
        
        -- Additive identification
        typ1 as additive_type,
        typ2 as additive_subtype,
        des as additive_name,
        vendoraddname as vendor_additive_name,
        purpose as purpose,
        refno as reference_number,
        
        -- Amount and design information
        amountdesign as design_amount,
        amount as actual_amount,
        unitlabel as units,
        
        -- Calculated totals (converted to US units)
        masstotalcalc / 0.45359237 as total_mass_of_additive_lb,
        voltotalcalc / 0.158987294928 as total_volume_of_additive_bbl,
        
        -- Concentration limits (converted to percentages)
        concmax / 0.01 as concentration_max_percent,
        concmin / 0.01 as concentration_min_percent,
        
        -- Additive density (complex conversion to API degrees)
        density as additive_density_api,  -- Note: Complex conversion applied in view
        
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