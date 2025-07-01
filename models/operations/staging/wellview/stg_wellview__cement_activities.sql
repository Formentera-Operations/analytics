{{ config(
    materialized='view',
    tags=['wellview', 'cement', 'casing', 'squeeze', 'plug', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVCEMENT') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idwell as well_id,
        idrec as record_id,
        
        -- Basic cement job information
        proposedoractual as proposed_or_actual_run,
        des as description,
        cementtyp as cement_type,
        cementsubtyp as cement_subtype,
        
        -- Job timing
        dttmstart as cementing_start_date,
        dttmend as cementing_end_date,
        durcalc / 0.0416666666666667 as duration_hours,
        
        -- String and wellbore relationships
        idrecstring as string_id,
        idrecstringtk as string_table_key,
        idrecwellbore as wellbore_id,
        idrecwellboretk as wellbore_table_key,
        idrecjob as job_id,
        idrecjobtk as job_table_key,
        
        -- Cement job details
        contractor as cementing_company,
        contractsupt as cementing_supervisor,
        objective as cement_objective,
        evalmethod as evaluation_method,
        deseval as cement_evaluation_results,
        
        -- Depths (converted to US units)
        depthtopcalc / 0.3048 as top_depth_ft,
        depthtvdtopcalc / 0.3048 as top_depth_tvd_ft,
        depthbtmcalc / 0.3048 as bottom_depth_ft,
        depthtvdbtmcalc / 0.3048 as bottom_depth_tvd_ft,
        
        -- Cement quantities (converted to US units)
        amtcementtotalcalc / 45.359237 as cement_amount_sacks,
        volpumpedtotalcalc / 0.158987294928 as volume_pumped_bbl,
        
        -- Cut and pull information
        dttmcutpull as cut_date,
        depthcutpull / 0.3048 as depth_cut_ft,
        reasoncutpull as reason_cut,
        notecutpull as cut_pull_note,
        
        -- Proposed cut and pull
        dttmpropcutpull as proposed_cut_date,
        
        -- Technical results
        resulttechnical as technical_result,
        resulttechnicaldetail as tech_result_details,
        resulttechnicalnote as tech_result_note,
        
        -- Phase and equipment references
        idrecjobprogramphasecalc as phase_id,
        idrecjobprogramphasecalctk as phase_table_key,
        idreclastrigcalc as last_rig_id,
        idreclastrigcalctk as last_rig_table_key,
        idreclastfailurecalc as last_failure_id,
        idreclastfailurecalctk as last_failure_table_key,
        
        -- Comments
        com as comment,

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