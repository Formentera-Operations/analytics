{{ config(
    materialized='view',
    tags=['wellview', 'job', 'supplies', 'transactions', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBSUPPLYAMT') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as supply_amount_id,
        idwell as well_id,
        idrecparent as job_supply_id,
        
        -- Transaction timing
        dttm as transaction_datetime,
        reportnocalc as report_number,
        
        -- Daily transaction amounts
        received as daily_received_quantity,
        consumed as daily_consumed_quantity,
        returned as daily_returned_quantity,
        
        -- Cumulative calculations
        receivedcumcalc as cumulative_received_quantity,
        consumedcumcalc as cumulative_consumed_quantity,
        returnedcumcalc as cumulative_returned_quantity,
        inventorycumcalc as cumulative_inventory_on_location,
        
        -- Cost information
        costor as cost_override,
        costcalc as daily_field_estimate_cost,
        costcumcalc as cumulative_field_estimate_cost,
        
        -- Related entities
        idrecjobsupportvessel as support_vessel_id,
        idrecjobsupportvesseltk as support_vessel_table_key,
        idrecitem as linked_item_id,
        idrecitemtk as linked_item_table_key,
        idreclastrigcalc as last_rig_id,
        idreclastrigcalctk as last_rig_table_key,
        
        -- Additional information
        note as transaction_notes,
        
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