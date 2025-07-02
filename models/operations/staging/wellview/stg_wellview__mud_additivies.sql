{{ config(
    materialized='view',
    tags=['wellview', 'drilling', 'mud', 'additives', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBMUDADD') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as mud_additive_id,
        idrecparent as job_id,
        idwell as well_id,
        
        -- Additive identification
        des as description,
        typ as additive_type,
        unitlabel as unit_label,
        unitsz as unit_size,
        note as notes,
        
        -- Vendor information
        vendor as vendor,
        vendorcode as vendor_code,
        vendorsubcode as vendor_subcode,
        
        -- Cost codes and description
        codedes as code_description,
        code1 as code_1,
        code2 as code_2,
        code3 as code_3,
        code4 as code_4,
        code5 as code_5,
        code6 as code_6,
        
        -- Cost information
        cost as unit_cost,
        costcalc as total_field_estimate_cost,
        
        -- Consumption planning and tracking
        consumedesign as planned_consumed_amount,
        consumedcalc as total_consumed,
        consumedesignvarcalc as planned_vs_actual_consumed_variance,
        
        -- Consumption per depth (converted to US units - per foot)
        consumedperdepthcalc / 3.28083989501312 as consumed_per_depth_per_ft,
        
        -- Inventory tracking
        receivedcalc as total_received,
        returnedcalc as total_returned,
        inventorycalc as inventory_on_location,
        
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