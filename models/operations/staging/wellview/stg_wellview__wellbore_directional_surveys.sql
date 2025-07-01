{{ config(
    materialized='view',
    tags=['wellview', 'wellbore', 'directional-surveys', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVWELLBOREDIRSURVEY') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as survey_id,
        idrecparent as wellbore_id,
        idwell as well_id,
        
        -- Survey metadata
        proposedoractual as proposed_or_actual,
        propversionno as proposed_version_number,
        dttm as survey_date,
        des as description,
        
        -- Survey control flags
        calcflag as use_for_calculations,
        definitive as is_definitive,
        
        -- Job reference
        idrecjob as job_id,
        idrecjobtk as job_table_key,
        
        -- Azimuth information
        azimuthnorthtyp as azimuth_north_type,
        azimuthtiein as azimuth_tie_in_degrees,
        azimuthcorrection as azimuth_correction_method,
        
        -- Angular measurements (in degrees)
        inclinationtiein as inclination_tie_in_degrees,
        declination as declination_degrees,
        convergence as convergence_degrees,
        
        -- Tie-in coordinates (converted to US units)
        mdtiein / 0.3048 as md_tie_in_ft,
        tvdtiein / 0.3048 as tvd_tie_in_ft,
        nstiein / 0.3048 as ns_tie_in_ft,
        ewtiein / 0.3048 as ew_tie_in_ft,
        vscalc / 0.3048 as vs_tie_in_ft,
        
        -- Validation information
        validateddttm as validated_date,
        validatedbyname as validated_by_name,
        validatedbycompany as validated_by_company,
        
        -- Correction methods
        depthcorrection as depth_correction_method,
        notecorrection as correction_notes,
        
        -- Comments
        com as comments,
        
        -- System fields
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockdate as system_lock_date,
        
        -- Fivetran metadata
        _fivetran_synced as fivetran_synced_at

    from source_data
)

select * from renamed