{{ config(
    materialized='view',
    tags=['wellview', 'casing', 'tally', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVCASCOMPTALLY') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as tally_id,
        idrecparent as component_id,
        idwell as well_id,
        sysseq as joint_sequence,

        -- Joint identification
        jointrun as joint_run_number,
        runnocalc as calculated_run_number,
        refid as reference_id,
        refno as reference_number,
        heatno as heat_number,

        -- Depths and positions (converted to US units)
        centralizersdes as centralizer_description,
        centralizersno as centralizer_count,
        extjewelry as external_jewelry,
        syscreatedate as created_at,

        -- Length measurements (converted to US units)
        syscreateuser as created_by,
        sysmoddate as modified_at,

        -- Centralizer information
        sysmoduser as modified_by,
        systag as system_tag,
        syslockdate as system_lock_date,

        -- External equipment
        syslockme as system_lock_me,

        -- Volume calculations (converted to US units)
        syslockchildren as system_lock_children,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,

        -- Weight calculations (converted to US units)
        _fivetran_synced as fivetran_synced_at,

        -- System fields
        depthtopcalc / 0.3048 as top_depth_ft,
        depthbtmcalc / 0.3048 as bottom_depth_ft,
        depthtvdtopcalc / 0.3048 as top_depth_tvd_ft,
        depthtvdbtmcalc / 0.3048 as bottom_depth_tvd_ft,
        length / 0.3048 as joint_length_ft,
        lengthcumcalc / 0.3048 as cumulative_length_ft,
        coalesce(centralized = 1, false) as has_centralizers,
        volumeinternalcalc / 0.158987294928 as internal_volume_bbl,
        volumeinternalcumcalc / 0.158987294928 as cumulative_internal_volume_bbl,
        volumedispcumcalc / 0.158987294928 as cumulative_displaced_volume_bbl,

        -- Fivetran fields
        weightcumcalc / 4448.2216152605 as cumulative_weight_klbf

    from source_data
)

select * from renamed
