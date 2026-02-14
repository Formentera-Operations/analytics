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
        priority as failure_priority,
        status1 as failure_status,

        -- Priority and status
        status2 as failure_status_detail,
        dttmstart as failure_date,
        dttmaction as action_date,

        -- Key dates
        dttmend as resolution_date,
        duractiontostartcalc as duration_action_to_start_days,
        durendtoactioncalc as duration_resolution_to_action_days,

        -- Duration calculations (already in days)
        durendtostartcalc as duration_resolution_to_start_days,
        durjobstarttostartcalc as duration_job_start_to_failure_days,
        estcost as estimated_failure_cost,
        actiontaken as action_taken,

        -- Pre-failure production rates (converted to US units)
        reportto as reported_to,
        idrecjob as repair_job_id,
        idrecjobtk as repair_job_table_key,
        idreczonecompletion as zone_completion_id,

        -- Post-failure production rates (converted to US units)
        idreczonecompletiontk as zone_completion_table_key,
        idrecprodsettingcalc as production_setting_id,
        idrecprodsettingcalctk as production_setting_table_key,
        idrecwellstatuscalc as pre_failure_well_status_id,

        -- Production impact calculations (converted to US units)
        idrecwellstatuscalctk as pre_failure_well_status_table_key,
        com as comments,
        usernum1 as user_number_1,
        usernum2 as user_number_2,

        -- Estimated reserve losses (converted to US units)
        usertxt1 as user_text_1,
        usertxt2 as user_text_2,
        usertxt3 as user_text_3,
        syscreatedate as created_at,

        -- Cost impact
        syscreateuser as created_by,

        -- Resolution details
        sysmoddate as modified_at,
        sysmoduser as modified_by,

        -- Related entities
        systag as system_tag,
        syslockdate as system_lock_date,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        _fivetran_synced as fivetran_synced_at,
        coalesce(regulatoryissue = 1, false) as is_regulatory_issue,

        -- Comments
        coalesce(performanceaffect = 1, false) as is_performance_affected,

        -- User fields
        rateoptimumoil / 0.1589873 as optimum_oil_rate_bbl_per_day,
        rateoptimumgas / 28.316846592 as optimum_gas_rate_mcf_per_day,
        rateoptimumcond / 0.1589873 as optimum_condensate_rate_bbl_per_day,
        rateoptimumwater / 0.1589873 as optimum_water_rate_bbl_per_day,
        ratefailoil / 0.1589873 as failure_oil_rate_bbl_per_day,

        -- System fields
        ratefailgas / 28.316846592 as failure_gas_rate_mcf_per_day,
        ratefailcond / 0.1589873 as failure_condensate_rate_bbl_per_day,
        ratefailwater / 0.1589873 as failure_water_rate_bbl_per_day,
        ratechangeoilcalc / 0.1589873 as oil_rate_impact_bbl_per_day,
        ratechangegascalc / 28.316846592 as gas_rate_impact_mcf_per_day,
        ratechangecondcalc / 0.1589873 as condensate_rate_impact_bbl_per_day,
        ratechangewatercalc / 0.1589873 as water_rate_impact_bbl_per_day,
        estreservelossoil / 0.158987294928 as estimated_oil_reserve_loss_bbl,
        estreservelossgas / 0.158987294928 as estimated_gas_reserve_loss_bbl,
        estreservelosscond / 0.158987294928 as estimated_condensate_reserve_loss_bbl,

        -- Fivetran fields
        estreservelosswater / 0.158987294928 as estimated_water_reserve_loss_bbl

    from source_data
)

select * from renamed
