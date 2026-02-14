{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'operations']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBREPORTCOSTGEN') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as cost_line_id,
        trim(idrecparent)::varchar as job_report_id,
        trim(idwell)::varchar as well_id,
        sysseq::int as sequence_number,

        -- cost information
        cost::float as field_estimate_cost,
        costcumcalc::float as cumulative_field_estimate_cost,

        -- account coding
        trim(code1)::varchar as main_account_id,
        trim(code2)::varchar as sub_account_id,
        trim(code3)::varchar as spend_category,
        trim(code4)::varchar as expense_type,
        trim(code5)::varchar as tangible_intangible,
        trim(code6)::varchar as afe_category,

        -- description and notes
        trim(des)::varchar as account_name,
        trim(note)::varchar as note,

        -- operational categorization
        trim(opscategory)::varchar as ops_category,
        trim(unschedtyp)::varchar as unscheduled_type,

        -- vendor information
        trim(vendor)::varchar as vendor_name,
        trim(vendorcode)::varchar as vendor_code,
        trim(vendorsubcode)::varchar as vendor_subcode,

        -- purchase order and work order information
        trim(pono)::varchar as purchase_order_number,
        trim(polineno)::varchar as purchase_order_line_number,
        trim(workorderno)::varchar as work_order_number,
        trim(ticketno)::varchar as ticket_number,
        trim(sn)::varchar as serial_number,

        -- status
        trim(status)::varchar as status,

        -- custom allocation fields
        trim(idrecphasecustom)::varchar as custom_phase_allocation_id,
        trim(idrecphasecustomtk)::varchar as custom_phase_allocation_table_key,
        trim(idrecafecustom)::varchar as custom_afe_allocation_id,
        trim(idrecafecustomtk)::varchar as custom_afe_allocation_table_key,
        trim(idrecintervalproblemcustom)::varchar as custom_interval_problem_allocation_id,
        trim(idrecintervalproblemcustomtk)::varchar as custom_interval_problem_allocation_table_key,

        -- interval problem allocation (proportion -> percentage)
        syscarryfwdp::boolean as carry_forward_to_next_parent,

        -- carry forward flag
        trim(usertxt1)::varchar as user_text_1,

        -- user fields
        userboolean1::boolean as user_boolean_1,
        trim(syscreateuser)::varchar as created_by,

        -- system / audit
        syscreatedate::timestamp_ntz as created_at,
        trim(sysmoduser)::varchar as modified_by,
        sysmoddate::timestamp_ntz as modified_at,
        trim(systag)::varchar as system_tag,
        syslockmeui::boolean as system_lock_me_ui,
        syslockchildrenui::boolean as system_lock_children_ui,
        syslockme::boolean as system_lock_me,
        syslockchildren::boolean as system_lock_children,
        syslockdate::timestamp_ntz as system_lock_date,
        _fivetran_deleted::boolean as _fivetran_deleted,

        -- ingestion metadata
        _fivetran_synced::timestamp_tz as _fivetran_synced,
        intervalproblempct / 0.01 as interval_problem_allocation_percent

    from source
),

filtered as (
    select *
    from renamed
    where
        coalesce(_fivetran_deleted, false) = false
        and cost_line_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['cost_line_id']) }} as daily_cost_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        daily_cost_sk,

        -- identifiers
        cost_line_id,
        job_report_id,
        well_id,
        sequence_number,

        -- cost information
        field_estimate_cost,
        cumulative_field_estimate_cost,

        -- account coding
        main_account_id,
        sub_account_id,
        spend_category,
        expense_type,
        tangible_intangible,
        afe_category,

        -- description and notes
        account_name,
        note,

        -- operational categorization
        ops_category,
        unscheduled_type,

        -- vendor information
        vendor_name,
        vendor_code,
        vendor_subcode,

        -- purchase order and work order information
        purchase_order_number,
        purchase_order_line_number,
        work_order_number,
        ticket_number,
        serial_number,

        -- status
        status,

        -- custom allocation fields
        custom_phase_allocation_id,
        custom_phase_allocation_table_key,
        custom_afe_allocation_id,
        custom_afe_allocation_table_key,
        custom_interval_problem_allocation_id,
        custom_interval_problem_allocation_table_key,

        -- interval problem allocation
        interval_problem_allocation_percent,

        -- carry forward flag
        carry_forward_to_next_parent,

        -- user fields
        user_text_1,
        user_boolean_1,

        -- system / audit
        created_by,
        created_at,
        modified_by,
        modified_at,
        system_tag,
        system_lock_me_ui,
        system_lock_children_ui,
        system_lock_me,
        system_lock_children,
        system_lock_date,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
