{{ config(
    materialized='view',
    tags=['wellview', 'job-reports', 'daily-costs', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBREPORTCOSTGEN') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as cost_line_id,
        idrecparent as job_report_id,
        idwell as well_id,
        sysseq as sequence_number,
        
        -- Cost information
        cost as field_estimate_cost,
        costcumcalc as cumulative_field_estimate_cost,
        
        -- Account coding
        code1 as main_account_id,
        code2 as sub_account_id,
        code3 as spend_category,
        code4 as expense_type,
        code5 as tangible_intangible,
        code6 as afe_category,
        
        -- Description and notes
        des as account_name,
        note as note,
        
        -- Operational categorization
        opscategory as ops_category,
        unschedtyp as unscheduled_type,
        
        -- Vendor information
        vendor as vendor_name,
        vendorcode as vendor_code,
        vendorsubcode as vendor_subcode,
        
        -- Purchase order and work order information
        pono as purchase_order_number,
        polineno as purchase_order_line_number,
        workorderno as work_order_number,
        ticketno as ticket_number,
        sn as serial_number,
        
        -- Status
        status as status,
        
        -- Custom allocation fields
        idrecphasecustom as custom_phase_allocation_id,
        idrecphasecustomtk as custom_phase_allocation_table_key,
        idrecafecustom as custom_afe_allocation_id,
        idrecafecustomtk as custom_afe_allocation_table_key,
        idrecintervalproblemcustom as custom_interval_problem_allocation_id,
        idrecintervalproblemcustomtk as custom_interval_problem_allocation_table_key,
        
        -- Interval problem allocation (converted from proportion to percentage)
        intervalproblempct / 0.01 as interval_problem_allocation_percent,
        
        -- Carry forward flag
        syscarryfwdp as carry_forward_to_next_parent,
        
        -- User fields
        usertxt1 as user_text_1,
        userboolean1 as user_boolean_1,
        
        -- System fields
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockdate as system_lock_date,
        
        -- Fivetran metadata
        _fivetran_synced as fivetran_synced_at

    from source_data
)

select * from renamed