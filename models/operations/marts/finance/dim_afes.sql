{{ config(
    materialized='table',
    indexes=[
        {'columns': ['afe_type_code']},
        {'columns': ['application_type_name']},
        {'columns': ['project_category']}
    ]
) }}

with afes_enhanced as (
    select 
        id as afe_id,
        code as afe_code,
        name as afe_name,
        full_name as afe_full_name,
        full_description as afe_full_description,
        
        -- AFE type information
        afe_type_id,
        afe_type_code,
        afe_type_label,
        afe_type_full_name,
        
        -- Application and budget information
        application_type_id,
        application_type_code,
        application_type_name,
        budget_usage_type_id,
        budget_usage_type_code,
        budget_usage_type_name,
        
        -- Related entities
        well_id,
        well_code,
        well_name,
        field_id,
        field_code,
        field_name,
        operating_group_id,
        operating_group_code,
        operating_group_name,
        default_company_code,
        default_company_name,
        
        -- Dates
        close_date,
        completion_date,
        
        -- Enhanced categorization based on AFE type
        case 
            when afe_type_code = 'EXP_WO' then 'Expense Workover'
            when afe_type_code = 'CAP_WO' then 'Capital Workover'
            when afe_type_code = 'CAP_FAC' then 'Capital Facility'
            when afe_type_code = 'CAP_DRL_CPL_FAC' then 'Drilling, Completion & Facilities'
            when afe_type_code = 'CAP_DRL_CPL_FAC_PRD' then 'Drilling, Completion, Facilities & Production'
            when afe_type_code = 'PA' then 'Plug & Abandonment'
            when afe_type_code = 'CAP_MID' then 'Midstream Capital'
            when afe_type_code = 'REG_ENV' then 'Regulatory & Environmental'
            else 'Other'
        end as project_category,
        
        -- Financial statement impact
        case 
            when afe_type_code in ('EXP_WO') then 'EXPENSE'
            when afe_type_code in ('CAP_WO', 'CAP_FAC', 'CAP_DRL_CPL_FAC', 'CAP_DRL_CPL_FAC_PRD', 'CAP_MID') then 'CAPITAL'
            when afe_type_code in ('PA') then 'ABANDONMENT'
            when afe_type_code in ('REG_ENV') then 'REGULATORY'
            else 'OTHER'
        end as financial_impact_type,
        
        -- Project status based on dates
        case 
            when completion_date is not null then 'Completed'
            when close_date is not null then 'Closed'
            else 'Active'
        end as project_status,
        
        -- Capital intensity classification
        case 
            when afe_type_code in ('CAP_DRL_CPL_FAC', 'CAP_DRL_CPL_FAC_PRD') then 'High Capital'
            when afe_type_code in ('CAP_WO', 'CAP_FAC', 'CAP_MID') then 'Medium Capital'
            when afe_type_code in ('EXP_WO') then 'Low Capital'
            when afe_type_code in ('PA', 'REG_ENV') then 'Minimal Capital'
            else 'Unknown'
        end as capital_intensity,
        
        -- Well association flag
        case 
            when well_id is not null then true
            else false
        end as has_well_association,
        
        -- Field-level project flag
        case 
            when application_type_name = 'Field' then true
            else false
        end as is_field_level_project
        
    from {{ ref('stg_oda__afe_v2') }}
)

select *,
    current_timestamp as dim_created_at,
    current_timestamp as dim_updated_at
from afes_enhanced