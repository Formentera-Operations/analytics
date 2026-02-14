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
        density as additive_density_api,
        usernum1 as user_number_1,

        -- Concentration limits (converted to percentages)
        usertxt1 as user_text_1,
        com as comment,

        -- Additive density (complex conversion to API degrees)
        syslockmeui as system_lock_me_ui,  -- Note: Complex conversion applied in view

        -- User fields
        syslockchildrenui as system_lock_children_ui,
        syslockme as system_lock_me,

        -- Comments
        syslockchildren as system_lock_children,

        -- System locking fields
        syslockdate as system_lock_date,
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,

        -- System tracking fields
        systag as system_tag,
        _fivetran_synced as fivetran_synced_at,
        masstotalcalc / 0.45359237 as total_mass_of_additive_lb,
        voltotalcalc / 0.158987294928 as total_volume_of_additive_bbl,
        concmax / 0.01 as concentration_max_percent,

        -- Fivetran metadata
        concmin / 0.01 as concentration_min_percent

    from source_data
)

select * from renamed
