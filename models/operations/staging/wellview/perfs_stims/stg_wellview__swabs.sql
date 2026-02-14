{{ config(
    materialized='view',
    tags=['wellview', 'completion', 'swab', 'testing', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVSWAB') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as swab_id,
        idwell as well_id,

        -- Swab operation details
        dttm as swab_date,
        proposedoractual as proposed_or_actual,
        contractor as swab_company,
        com as comments,

        -- Operational references
        idrecjob as job_id,
        idrecjobtk as job_table_key,
        idrecwellbore as wellbore_id,
        idrecwellboretk as wellbore_table_key,
        idreczonecompletion as completion_zone_id,
        idreczonecompletiontk as completion_zone_table_key,

        -- Volume recoveries (converted to US units)
        -- Total volumes in barrels
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,

        -- Gas volume in thousand cubic feet
        sysmoduser as modified_by,

        -- System fields
        systag as system_tag,
        syslockdate as system_lock_date,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        _fivetran_synced as fivetran_synced_at,
        voltotalcalc / 0.158987294928 as total_volume_recovered_bbl,
        voltotaloilcalc / 0.158987294928 as total_oil_recovered_bbl,
        voltotalbswcalc / 0.158987294928 as total_bsw_recovered_bbl,

        -- Fivetran fields
        voltotalgascalc / 28.316846592 as total_gas_volume_mcf

    from source_data
)

select * from renamed
