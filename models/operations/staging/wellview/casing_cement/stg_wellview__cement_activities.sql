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
        idrecstring as string_id,

        -- String and wellbore relationships
        idrecstringtk as string_table_key,
        idrecwellbore as wellbore_id,
        idrecwellboretk as wellbore_table_key,
        idrecjob as job_id,
        idrecjobtk as job_table_key,
        contractor as cementing_company,

        -- Cement job details
        contractsupt as cementing_supervisor,
        objective as cement_objective,
        evalmethod as evaluation_method,
        deseval as cement_evaluation_results,
        dttmcutpull as cut_date,

        -- Depths (converted to US units)
        reasoncutpull as reason_cut,
        notecutpull as cut_pull_note,
        dttmpropcutpull as proposed_cut_date,
        resulttechnical as technical_result,

        -- Cement quantities (converted to US units)
        resulttechnicaldetail as tech_result_details,
        resulttechnicalnote as tech_result_note,

        -- Cut and pull information
        idrecjobprogramphasecalc as phase_id,
        idrecjobprogramphasecalctk as phase_table_key,
        idreclastrigcalc as last_rig_id,
        idreclastrigcalctk as last_rig_table_key,

        -- Proposed cut and pull
        idreclastfailurecalc as last_failure_id,

        -- Technical results
        idreclastfailurecalctk as last_failure_table_key,
        com as comment,
        syslockmeui as system_lock_me_ui,

        -- Phase and equipment references
        syslockchildrenui as system_lock_children_ui,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockdate as system_lock_date,
        syscreatedate as created_at,
        syscreateuser as created_by,

        -- Comments
        sysmoddate as modified_at,

        -- System locking fields
        sysmoduser as modified_by,
        systag as system_tag,
        _fivetran_synced as fivetran_synced_at,
        durcalc / 0.0416666666666667 as duration_hours,
        depthtopcalc / 0.3048 as top_depth_ft,

        -- System tracking fields
        depthtvdtopcalc / 0.3048 as top_depth_tvd_ft,
        depthbtmcalc / 0.3048 as bottom_depth_ft,
        depthtvdbtmcalc / 0.3048 as bottom_depth_tvd_ft,
        amtcementtotalcalc / 45.359237 as cement_amount_sacks,
        volpumpedtotalcalc / 0.158987294928 as volume_pumped_bbl,

        -- Fivetran metadata
        depthcutpull / 0.3048 as depth_cut_ft

    from source_data
)

select * from renamed
