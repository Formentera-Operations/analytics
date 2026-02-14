{{ config(
    materialized='view',
    tags=['wellview', 'job', 'report', 'costs', 'recurring', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBREPORTCOSTRENTAL') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as recurring_cost_id,
        idwell as well_id,
        idrecparent as daily_report_id,

        -- Rental item reference
        idrecjobrentalitem as rental_item_id,
        idrecjobrentalitemtk as rental_item_table_key,

        -- System behavior
        dttmstartcalc as report_start_datetime,

        -- Calculated period and description
        dttmendcalc as report_end_datetime,
        descalc as cost_description,
        qty as quantity,

        -- Billing parameters
        useother as other_rate_1,
        costrentalcalc as rental_field_estimate,
        costonetime as additional_one_time_amount,
        costcumcalc as cumulative_field_estimate,
        opscategory as operations_category,
        unschedtyp as unscheduled_type,

        -- Cost calculations
        sn as serial_number,
        ticketno as ticket_number,
        workorderno as work_order_number,

        -- Operational classification
        status as cost_status,
        note as cost_notes,

        -- Equipment tracking
        code1calc as cost_code_1,
        code2calc as cost_code_2,
        code3calc as cost_code_3,
        code4calc as cost_code_4,
        code5calc as cost_code_5,

        -- Cost coding (calculated from rental item)
        code6calc as cost_code_6,
        vendorcalc as vendor_name,
        vendorcodecalc as vendor_code,
        vendorsubcodecalc as vendor_subcode,
        ponocalc as purchase_order_number,
        polinenocalc as purchase_order_line_number,

        -- Vendor information (calculated from rental item)
        poamtcalc as purchase_order_amount,
        idrecphasecustom as custom_phase_allocation_id,
        idrecphasecustomtk as custom_phase_allocation_table_key,

        -- Purchase order information (calculated from rental item)
        idrecafecustom as custom_afe_allocation_id,
        idrecafecustomtk as custom_afe_allocation_table_key,
        idrecintervalproblemcustom as custom_interval_problem_allocation_id,

        -- Custom allocations
        idrecintervalproblemcustomtk as custom_interval_problem_allocation_table_key,
        idrecjobcalc as job_id,
        idrecjobcalctk as job_table_key,
        userboolean1calc as user_boolean_1,
        usertxt1 as user_text_1,
        sysseq as sequence_number,
        syscreatedate as created_at,

        -- Related entities
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,

        -- System fields
        syslockdate as system_lock_date,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        _fivetran_synced as fivetran_synced_at,
        coalesce(syscarryfwdp = 1, false) as carry_forward_to_next_report,
        coalesce(useday = 1, false) as use_day_charge,
        coalesce(usestandby = 1, false) as use_standby_charge,
        usehour / 0.0416666666666667 as hours_charged,
        usedepth / 0.3048 as depth_charged_ft,

        -- Fivetran fields
        intervalproblempct / 0.01 as interval_problem_allocation_percent

    from source_data
)

select * from renamed
