{{ config(
    materialized='view',
    tags=['wellview', 'personnel', 'daily-operations', 'headcount', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBREPORTPERSONNELCOUNT') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idwell as well_id,
        idrecparent as parent_record_id,
        idrec as record_id,
        
        -- Personnel information
        employeename as employee_name,
        employeetyp as employee_type,
        company as company_name,
        companytyp as company_type,
        headcount as head_count,
        
        -- Working hours (converted to US units)
        durationworkreg / 0.0416666666666667 as regular_working_hours,
        durationworkot / 0.0416666666666667 as overtime_working_hours,
        durationworktotcalc / 0.0416666666666667 as total_working_hours,
        
        -- Configuration flags
        syscarryfwdp as carry_forward_to_next_parent,
        exclude as exclude_from_calculations,
        
        -- Reference information
        refderrick as derrick_reference,
        note as note,
        
        -- Sequence
        sysseq as sequence_number,

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