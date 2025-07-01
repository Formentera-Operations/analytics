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
        depthtopcalc / 0.3048 as top_depth_ft,
        depthbtmcalc / 0.3048 as bottom_depth_ft,
        depthtvdtopcalc / 0.3048 as top_depth_tvd_ft,
        depthtvdbtmcalc / 0.3048 as bottom_depth_tvd_ft,
        
        -- Length measurements (converted to US units)
        length / 0.3048 as joint_length_ft,
        lengthcumcalc / 0.3048 as cumulative_length_ft,
        
        -- Centralizer information
        case when centralized = 1 then true else false end as has_centralizers,
        centralizersdes as centralizer_description,
        centralizersno as centralizer_count,
        
        -- External equipment
        extjewelry as external_jewelry,
        
        -- Volume calculations (converted to US units)
        volumeinternalcalc / 0.158987294928 as internal_volume_bbl,
        volumeinternalcumcalc / 0.158987294928 as cumulative_internal_volume_bbl,
        volumedispcumcalc / 0.158987294928 as cumulative_displaced_volume_bbl,
        
        -- Weight calculations (converted to US units)
        weightcumcalc / 4448.2216152605 as cumulative_weight_klbf,
        
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