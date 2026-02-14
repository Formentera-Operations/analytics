{{ config(
    materialized='view',
    tags=['wellview', 'tubing', 'components', 'tally', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVTUBCOMPTALLY') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as tally_id,
        idrecparent as tubing_component_id,
        idwell as well_id,
        sysseq as sequence_number,

        -- Joint and run information
        jointrun as joint_run_number,
        runnocalc as run_number_calc,

        -- Lengths (converted to US units)
        centralized as is_centralized,
        centralizersdes as centralizer_description,

        -- Depths (converted to US units)
        centralizersno as centralizer_number,
        extjewelry as external_jewelry,
        refid as reference_id,
        refno as reference_number,

        -- Volumes (converted to US units)
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,

        -- Weight (converted to US units)
        sysmoduser as modified_by,

        -- Centralizer information
        systag as system_tag,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,

        -- External jewelry
        syslockme as system_lock_me,

        -- Reference information
        syslockchildren as system_lock_children,
        syslockdate as system_lock_date,

        -- System fields
        _fivetran_synced as fivetran_synced_at,
        length / 0.3048 as joint_length_ft,
        lengthcumcalc / 0.3048 as cumulative_length_ft,
        depthtopcalc / 0.3048 as top_depth_ft,
        depthbtmcalc / 0.3048 as bottom_depth_ft,
        depthtvdtopcalc / 0.3048 as top_depth_tvd_ft,
        depthtvdbtmcalc / 0.3048 as bottom_depth_tvd_ft,
        volumeinternalcalc / 0.158987294928 as internal_volume_bbl,
        volumeinternalcumcalc / 0.158987294928 as cumulative_internal_volume_bbl,
        volumedispcumcalc / 0.158987294928 as cumulative_displaced_volume_bbl,

        -- Fivetran metadata
        weightcumcalc / 4448.2216152605 as cumulative_weight_klbf

    from source_data
)

select * from renamed
