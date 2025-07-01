{{ config(
    materialized='view',
    tags=['wellview', 'job', 'afe', 'financial', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBAFE') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as job_afe_id,
        idwell as well_id,
        idrecparent as job_id,
        
        -- AFE identification
        afenumber as afe_number,
        afenumbersupp as supplemental_afe_number,
        afecosttypcalc as afe_cost_type,
        afestatus as afe_status,
        
        -- Project information
        projectname as project_name,
        projectrefnumber as project_reference_number,
        contactname as contact_name,
        typ as afe_type,
        costtyp as cost_type,
        
        -- Dates
        dttmafe as afe_date,
        dttmafeclose as afe_close_date,
        
        -- Working interest (converted to US units)
        workingint / 0.01 as working_interest_percent,
        workingintnote as working_interest_notes,
        
        -- Control flags
        case when exclude = 1 then true else false end as exclude_from_cost_calculations,
        
        -- AFE amounts (gross)
        afeamtcalc as total_afe_amount,
        afesupamtcalc as total_afe_supplemental_amount,
        afetotalcalc as total_afe_plus_supplemental_amount,
        
        -- AFE amounts (normalized)
        afeamtnormcalc as normalized_total_afe_amount,
        afesupamtnormcalc as normalized_total_afe_supplemental_amount,
        afetotalnormcalc as normalized_total_afe_plus_supplemental_amount,
        
        -- AFE amounts (net)
        afeamtnetcalc as net_total_afe_amount,
        afesupamtnetcalc as net_total_afe_supplemental_amount,
        afetotalnetcalc as net_total_afe_plus_supplemental_amount,
        
        -- Field estimates
        costtotalcalc as total_field_estimate,
        costnormtotalcalc as normalized_total_field_estimate,
        costnettotalcalc as net_total_field_estimate,
        
        -- Forecasts
        costforecastcalc as forecast_amount,
        costnormforecastcalc as normalized_forecast_amount,
        costnetforecastcalc as net_forecast_amount,
        
        -- Final invoices
        finalinvoicetotalcalc as total_final_invoice,
        finalinvoicetotalnormcalc as normalized_total_final_invoice,
        finalinvoicetotalnetcalc as net_total_final_invoice,
        
        -- Variances - AFE vs Field
        variancefieldcalc as afe_minus_field_estimate_variance,
        variancenormfieldcalc as normalized_afe_minus_field_estimate_variance,
        variancenetfieldcalc as net_afe_minus_field_estimate_variance,
        
        -- Variances - AFE vs Final
        varianceafefinalcalc as afe_minus_final_invoice_variance,
        variancenormafefinalcalc as normalized_afe_minus_final_invoice_variance,
        variancenetafefinalcalc as net_afe_minus_final_invoice_variance,
        
        -- Variances - Field vs Final
        variancefieldfinalcalc as field_minus_final_invoice_variance,
        variancenormfieldfinalcalc as normalized_field_minus_final_invoice_variance,
        variancenetfieldfinalcalc as net_field_minus_final_invoice_variance,
        
        -- Comments
        com as comments,
        
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