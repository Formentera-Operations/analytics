{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'operations']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBREPORTPERSONNELCOUNT') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as record_id,
        trim(idrecparent)::varchar as job_report_id,
        trim(idwell)::varchar as well_id,
        sysseq::int as sequence_number,

        -- personnel information
        trim(employeename)::varchar as employee_name,
        trim(employeetyp)::varchar as employee_type,
        trim(company)::varchar as company_name,
        trim(companytyp)::varchar as company_type,
        headcount::int as head_count,

        -- working hours (converted from days to hours)
        {{ wv_days_to_hours('durationworkreg') }} as regular_working_hours,
        {{ wv_days_to_hours('durationworkot') }} as overtime_working_hours,
        {{ wv_days_to_hours('durationworktotcalc') }} as total_working_hours,

        -- configuration flags
        syscarryfwdp::boolean as carry_forward_to_next_parent,
        exclude::boolean as exclude_from_calculations,
        trim(refderrick)::varchar as derrick_reference,

        -- notes
        trim(note)::varchar as note,

        -- system / audit
        trim(syscreateuser)::varchar as created_by,
        syscreatedate::timestamp_ntz as created_at,
        trim(sysmoduser)::varchar as modified_by,
        sysmoddate::timestamp_ntz as modified_at,
        trim(systag)::varchar as system_tag,
        syslockmeui::boolean as system_lock_me_ui,
        syslockchildrenui::boolean as system_lock_children_ui,
        syslockme::boolean as system_lock_me,
        syslockchildren::boolean as system_lock_children,
        syslockdate::timestamp_ntz as system_lock_date,

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
        and record_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['record_id']) }} as daily_personnel_log_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        daily_personnel_log_sk,

        -- identifiers
        record_id,
        job_report_id,
        well_id,
        sequence_number,

        -- personnel information
        employee_name,
        employee_type,
        company_name,
        company_type,
        head_count,

        -- working hours
        regular_working_hours,
        overtime_working_hours,
        total_working_hours,

        -- configuration flags
        carry_forward_to_next_parent,
        exclude_from_calculations,
        derrick_reference,

        -- notes
        note,

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
