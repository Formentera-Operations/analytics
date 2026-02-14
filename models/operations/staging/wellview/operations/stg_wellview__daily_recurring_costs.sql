{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'operations']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBREPORTCOSTRENTAL') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as recurring_cost_id,
        trim(idrecparent)::varchar as daily_report_id,
        trim(idwell)::varchar as well_id,
        sysseq::int as sequence_number,

        -- rental item reference
        trim(idrecjobrentalitem)::varchar as rental_item_id,
        trim(idrecjobrentalitemtk)::varchar as rental_item_table_key,

        -- calculated period and description
        dttmstartcalc::timestamp_ntz as report_start_datetime,
        dttmendcalc::timestamp_ntz as report_end_datetime,
        trim(descalc)::varchar as cost_description,

        -- billing parameters
        qty::float as quantity,
        coalesce(useday = 1, false) as use_day_charge,
        coalesce(usestandby = 1, false) as use_standby_charge,
        {{ wv_days_to_hours('usehour') }} as hours_charged,
        {{ wv_meters_to_feet('usedepth') }} as depth_charged_ft,
        useother::float as other_rate_1,

        -- cost calculations
        costrentalcalc::float as rental_field_estimate,
        costonetime::float as additional_one_time_amount,
        costcumcalc::float as cumulative_field_estimate,

        -- operational classification
        trim(opscategory)::varchar as operations_category,
        trim(unschedtyp)::varchar as unscheduled_type,
        trim(status)::varchar as cost_status,
        trim(note)::varchar as cost_notes,

        -- equipment tracking
        trim(sn)::varchar as serial_number,
        trim(ticketno)::varchar as ticket_number,
        trim(workorderno)::varchar as work_order_number,

        -- cost coding (calculated from rental item)
        trim(code1calc)::varchar as cost_code_1,
        trim(code2calc)::varchar as cost_code_2,
        trim(code3calc)::varchar as cost_code_3,
        trim(code4calc)::varchar as cost_code_4,
        trim(code5calc)::varchar as cost_code_5,
        trim(code6calc)::varchar as cost_code_6,

        -- vendor information (calculated from rental item)
        trim(vendorcalc)::varchar as vendor_name,
        trim(vendorcodecalc)::varchar as vendor_code,
        trim(vendorsubcodecalc)::varchar as vendor_subcode,

        -- purchase order information (calculated from rental item)
        trim(ponocalc)::varchar as purchase_order_number,
        trim(polinenocalc)::varchar as purchase_order_line_number,
        poamtcalc::float as purchase_order_amount,

        -- custom allocations
        trim(idrecphasecustom)::varchar as custom_phase_allocation_id,
        trim(idrecphasecustomtk)::varchar as custom_phase_allocation_table_key,
        trim(idrecafecustom)::varchar as custom_afe_allocation_id,
        trim(idrecafecustomtk)::varchar as custom_afe_allocation_table_key,
        trim(idrecintervalproblemcustom)::varchar as custom_interval_problem_allocation_id,
        trim(idrecintervalproblemcustomtk)::varchar as custom_interval_problem_allocation_table_key,

        -- interval problem allocation (proportion -> percentage)
        intervalproblempct / 0.01 as interval_problem_allocation_percent,

        -- carry forward flag
        coalesce(syscarryfwdp = 1, false) as carry_forward_to_next_report,

        -- related entities
        trim(idrecjobcalc)::varchar as job_id,
        trim(idrecjobcalctk)::varchar as job_table_key,

        -- user fields
        userboolean1calc::boolean as user_boolean_1,
        trim(usertxt1)::varchar as user_text_1,

        -- system / audit
        trim(syscreateuser)::varchar as created_by,
        syscreatedate::timestamp_ntz as created_at,
        trim(sysmoduser)::varchar as modified_by,
        sysmoddate::timestamp_ntz as modified_at,
        trim(systag)::varchar as system_tag,
        syslockdate::timestamp_ntz as system_lock_date,
        syslockme::boolean as system_lock_me,
        syslockchildren::boolean as system_lock_children,
        syslockmeui::boolean as system_lock_me_ui,
        syslockchildrenui::boolean as system_lock_children_ui,

        -- ingestion metadata
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_tz as _fivetran_synced

    from source
),

filtered as (
    select *
    from renamed
    where
        coalesce(_fivetran_deleted, false) = false
        and recurring_cost_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['recurring_cost_id']) }} as daily_recurring_cost_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        daily_recurring_cost_sk,

        -- identifiers
        recurring_cost_id,
        daily_report_id,
        well_id,
        sequence_number,

        -- rental item reference
        rental_item_id,
        rental_item_table_key,

        -- calculated period and description
        report_start_datetime,
        report_end_datetime,
        cost_description,

        -- billing parameters
        quantity,
        use_day_charge,
        use_standby_charge,
        hours_charged,
        depth_charged_ft,
        other_rate_1,

        -- cost calculations
        rental_field_estimate,
        additional_one_time_amount,
        cumulative_field_estimate,

        -- operational classification
        operations_category,
        unscheduled_type,
        cost_status,
        cost_notes,

        -- equipment tracking
        serial_number,
        ticket_number,
        work_order_number,

        -- cost coding
        cost_code_1,
        cost_code_2,
        cost_code_3,
        cost_code_4,
        cost_code_5,
        cost_code_6,

        -- vendor information
        vendor_name,
        vendor_code,
        vendor_subcode,

        -- purchase order information
        purchase_order_number,
        purchase_order_line_number,
        purchase_order_amount,

        -- custom allocations
        custom_phase_allocation_id,
        custom_phase_allocation_table_key,
        custom_afe_allocation_id,
        custom_afe_allocation_table_key,
        custom_interval_problem_allocation_id,
        custom_interval_problem_allocation_table_key,
        interval_problem_allocation_percent,

        -- flags
        carry_forward_to_next_report,

        -- related entities
        job_id,
        job_table_key,

        -- user fields
        user_boolean_1,
        user_text_1,

        -- system / audit
        created_by,
        created_at,
        modified_by,
        modified_at,
        system_tag,
        system_lock_date,
        system_lock_me,
        system_lock_children,
        system_lock_me_ui,
        system_lock_children_ui,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
