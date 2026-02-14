{{ config(
    materialized='view',
    tags=['wellview', 'rig', 'pump', 'operations', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBRIGPUMPOP') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as pump_operation_id,
        idwell as well_id,
        idrecparent as rig_pump_id,

        -- Operational period
        dttmstart as operation_start_datetime,
        dttmend as operation_end_datetime,

        -- Pump configuration (converted to US units)
        syscreatedate as created_at,
        syscreateuser as created_by,

        -- Volume per stroke (converted to US units)
        sysmoddate as modified_at,
        sysmoduser as modified_by,

        -- Operational time tracking (converted to US units)
        systag as system_tag,
        syslockdate as system_lock_date,

        -- Performance metrics (converted to US units)
        syslockme as system_lock_me,

        -- System fields
        syslockchildren as system_lock_children,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        _fivetran_synced as fivetran_synced_at,
        szliner / 0.0254 as liner_size_in,
        pressuremax / 6.894757 as maximum_pressure_psi,
        volperstroke / 0.158987294928 as volume_per_stroke_override_bbl_per_stroke,
        volperstrokecalc / 0.158987294928 as volume_per_stroke_calculated_bbl_per_stroke,
        tmcirccalc / 0.0416666666666667 as circulating_time_hours,
        tmdrillcalc / 0.0416666666666667 as drilling_time_hours,

        -- Fivetran fields
        volefficiencycalc / 0.01 as volumetric_efficiency_percent

    from source_data
)

select * from renamed
