{{ config(
    materialized='view',
    tags=['wellview', 'job', 'supplies', 'inventory', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBSUPPLY') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as job_supply_id,
        idwell as well_id,
        idrecparent as job_id,
        
        -- Supply description
        des as supply_item_description,
        typ as supply_type,
        note as supply_notes,
        
        -- Unit information
        unitlabel as unit_label,
        unitsz as unit_size,
        
        -- Environmental and energy information
        environmenttyp as environmental_type,
        energyfactor as energy_factor_joules,
        
        -- Vendor information
        vendor as vendor_name,
        vendorcode as vendor_code,
        vendorsubcode as vendor_subcode,
        
        -- Cost information
        cost as unit_cost,
        costcalc as total_field_estimate_cost,
        
        -- Quantity tracking
        consumedesign as planned_consumed_amount,
        receivedcalc as total_received_quantity,
        consumedcalc as total_consumed_quantity,
        returnedcalc as total_returned_quantity,
        inventorycalc as inventory_on_location,
        consumedesignvarcalc as planned_vs_actual_consumed_variance,
        
        -- Cost coding system
        codedes as cost_code_description,
        code1 as cost_code_1,
        code2 as cost_code_2,
        code3 as cost_code_3,
        code4 as cost_code_4,
        code5 as cost_code_5,
        code6 as cost_code_6,
        
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