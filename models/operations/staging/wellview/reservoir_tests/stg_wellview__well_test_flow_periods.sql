{{ config(
    materialized='view',
    tags=['wellview', 'completion', 'well_test', 'transient', 'flow_period', 'testing', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVWELLTESTTRANSFLOWPER') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as flow_period_id,
        idrecparent as well_test_id,
        idwell as well_id,

        -- Flow period timing
        dttmstart as flow_period_start_date,
        dttmend as flow_period_end_date,
        typ as flow_period_type,
        idrecgaugeused as gauge_used_id,

        -- Gauge reference
        idrecgaugeusedtk as gauge_used_table_key,
        phwaterend as water_ph_end,

        -- Choke and operational parameters
        com as comments,
        syscreatedate as created_at,

        -- Fluid level measurements (converted to feet)
        syscreateuser as created_by,
        sysmoddate as modified_at,

        -- Initial pressures (converted to PSI)
        sysmoduser as modified_by,
        systag as system_tag,
        syslockdate as system_lock_date,
        syslockme as system_lock_me,

        -- End pressures (converted to PSI)
        syslockchildren as system_lock_children,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        _fivetran_synced as fivetran_synced_at,

        -- Initial temperatures (converted to Fahrenheit)
        durcalc / 0.0416666666666667 as duration_hours,
        szdiachoke / 0.000396875 as choke_diameter_64ths,
        efficiency / 0.01 as efficiency_percent,
        depthfluidlevelend / 0.3048 as fluid_level_end_depth_ft,

        -- End temperatures (converted to Fahrenheit)
        depthtvdfluidlevelendcalc / 0.3048 as fluid_level_end_depth_tvd_ft,
        presbhinit / 6.894757 as bottomhole_pressure_initial_psi,
        prescasinit / 6.894757 as casing_pressure_initial_psi,
        prestubinit / 6.894757 as tubing_pressure_initial_psi,

        -- Production rates (converted to field units)
        pressepinit / 6.894757 as separator_pressure_initial_psi,
        presbhend / 6.894757 as bottomhole_pressure_end_psi,
        prescasend / 6.894757 as casing_pressure_end_psi,
        prestubend / 6.894757 as tubing_pressure_end_psi,
        pressepend / 6.894757 as separator_pressure_end_psi,

        -- Total volumes (converted to field units)
        tempbhinit / 0.555555555555556 + 32 as bottomhole_temperature_initial_f,
        tempcasinit / 0.555555555555556 + 32 as casing_temperature_initial_f,
        temptubinit / 0.555555555555556 + 32 as tubing_temperature_initial_f,
        tempsepinit / 0.555555555555556 + 32 as separator_temperature_initial_f,

        -- Fluid properties (converted to percentages and field units)
        tempbhend / 0.555555555555556 + 32 as bottomhole_temperature_end_f,
        tempcasend / 0.555555555555556 + 32 as casing_temperature_end_f,
        temptubend / 0.555555555555556 + 32 as tubing_temperature_end_f,
        tempsepend / 0.555555555555556 + 32 as separator_temperature_end_f,
        rateoilend / 0.1589873 as oil_rate_end_bbl_per_day,

        -- Density measurements (converted to field units)
        ratewaterend / 0.1589873 as water_rate_end_bbl_per_day,
        ratecondend / 0.1589873 as condensate_rate_end_bbl_per_day,

        -- Comments
        rategasend / 28.316846592 as gas_rate_end_mcf_per_day,

        -- System fields
        rategasliftgas / 28.316846592 as gas_lift_rate_mcf_per_day,
        volumeoiltotal / 0.158987294928 as total_oil_volume_bbl,
        volumewatertotal / 0.158987294928 as total_water_volume_bbl,
        volumecondtotal / 0.158987294928 as total_condensate_volume_bbl,
        volumegastotal / 28.316846592 as total_gas_volume_mcf,
        bswend / 0.01 as basic_sediments_water_end_percent,
        sandcutend / 0.01 as sand_cut_end_percent,
        salinitywaterend / 1e-06 as water_salinity_end_ppm,
        h2send / 1e-06 as h2s_end_ppm,
        power(nullif(densityoilend, 0), -1) / 7.07409872233005e-06 - 131.5 as oil_density_end_api,

        -- Fivetran fields
        densitygasend / 0.01601846250554 as gas_density_end_lb_per_1000ft3

    from source_data
)

select * from renamed
