{{ config(
    materialized='view',
    tags=['wellview', 'production', 'failures', 'problems', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVPROBLEM') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as production_failure_id,
        idwell as well_id,
        
        -- Failure classification
        typ as failure_type,
        typcategory as failure_type_category,
        typdetail as failure_type_detail,
        des as failure_description,
        
        -- Failure characteristics
        failuresystem as failure_system,
        failuresystemcategory as failure_system_category,
        failuresymptom as failure_symptom,
        cause as cause_of_failure,
        causecategory as cause_category,
        causedetail as cause_detail,
        causecom as cause_comments,
        
        -- Failure impact flags
        case when regulatoryissue = 1 then true else false end as is_regulatory_issue,
        case when performanceaffect = 1 then true else false end as is_performance_affected,
        
        -- Priority and status
        priority as failure_priority,
        status1 as failure_status,
        status2 as failure_status_detail,
        
        -- Key dates
        dttmstart as failure_date,
        dttmaction as action_date,
        dttmend as resolution_date,
        
        -- Duration calculations (already in days)
        duractiontostartcalc as duration_action_to_start_days,
        durendtoactioncalc as duration_resolution_to_action_days,
        durendtostartcalc as duration_resolution_to_start_days,
        durjobstarttostartcalc as duration_job_start_to_failure_days,
        
        -- Pre-failure production rates (converted to US units)
        rateoptimumoil / 0.1589873 as optimum_oil_rate_bbl_per_day,
        rateoptimumgas / 28.316846592 as optimum_gas_rate_mcf_per_day,
        rateoptimumcond / 0.1589873 as optimum_condensate_rate_bbl_per_day,
        rateoptimumwater / 0.1589873 as optimum_water_rate_bbl_per_day,
        
        -- Post-failure production rates (converted to US units)
        ratefailoil / 0.1589873 as failure_oil_rate_bbl_per_day,
        ratefailgas / 28.316846592 as failure_gas_rate_mcf_per_day,
        ratefailcond / 0.1589873 as failure_condensate_rate_bbl_per_day,
        ratefailwater / 0.1589873 as failure_water_rate_bbl_per_day,
        
        -- Production impact calculations (converted to US units)
        ratechangeoilcalc / 0.1589873 as oil_rate_impact_bbl_per_day,
        ratechangegascalc / 28.316846592 as gas_rate_impact_mcf_per_day,
        ratechangecondcalc / 0.1589873 as condensate_rate_impact_bbl_per_day,
        ratechangewatercalc / 0.1589873 as water_rate_impact_bbl_per_day,
        
        -- Estimated reserve losses (converted to US units)
        estreservelossoil / 0.158987294928 as estimated_oil_reserve_loss_bbl,
        estreservelossgas / 0.158987294928 as estimated_gas_reserve_loss_bbl,
        estreservelosscond / 0.158987294928 as estimated_condensate_reserve_loss_bbl,
        estreservelosswater / 0.158987294928 as estimated_water_reserve_loss_bbl,
        
        -- Cost impact
        estcost as estimated_failure_cost,
        
        -- Resolution details
        actiontaken as action_taken,
        reportto as reported_to,
        
        -- Related entities
        idrecjob as repair_job_id,
        idrecjobtk as repair_job_table_key,
        idreczonecompletion as zone_completion_id,
        idreczonecompletiontk as zone_completion_table_key,
        idrecprodsettingcalc as production_setting_id,
        idrecprodsettingcalctk as production_setting_table_key,
        idrecwellstatuscalc as pre_failure_well_status_id,
        idrecwellstatuscalctk as pre_failure_well_status_table_key,
        
        -- Comments
        com as comments,
        
        -- User fields
        usernum1 as user_number_1,
        usernum2 as user_number_2,
        usertxt1 as user_text_1,
        usertxt2 as user_text_2,
        usertxt3 as user_text_3,
        
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