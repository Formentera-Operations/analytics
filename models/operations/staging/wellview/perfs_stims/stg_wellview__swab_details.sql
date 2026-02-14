{{ config(
    materialized='view',
    tags=['wellview', 'completion', 'swab', 'testing', 'staging', 'details']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVSWABDETAILS') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as swab_detail_id,
        idrecparent as swab_id,
        idwell as well_id,

        -- Swab detail timing
        dttm as swab_detail_date,
        swabno as swab_number,
        ph as ph_value,

        -- Pressures (converted to PSI)
        com as comments,
        syscreatedate as created_at,

        -- Temperature (converted to Fahrenheit)
        syscreateuser as created_by,

        -- Depths (converted to feet)
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,
        syslockdate as system_lock_date,

        -- Tank measurements (converted to inches)
        syslockme as system_lock_me,

        -- Fluid volumes (converted to barrels)
        syslockchildren as system_lock_children,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        _fivetran_synced as fivetran_synced_at,
        tmswab / 0.0416666666666667 as swabbing_time_hours,
        prestub / 6.894757 as tubing_pressure_psi,

        -- Gas volumes and rates (converted to MCF and MCF/day)
        prescas / 6.894757 as casing_pressure_psi,
        tempwh / 0.555555555555556 + 32 as wellhead_temperature_f,
        depthfluidlevel / 0.3048 as tagged_fluid_level_ft,

        -- Oil rate (converted to BBL/day)
        depthpull / 0.3048 as pull_depth_ft,

        -- Fluid properties (converted to percentages and API)
        depthtvdfluidlevelcalc / 0.3048 as fluid_level_tvd_ft,
        depthtvdpullcalc / 0.3048 as pull_depth_tvd_ft,
        tankgauge / 0.0254 as tank_gauge_inches,
        volfluidrec / 0.158987294928 as recovered_fluid_volume_bbl,

        -- Density (converted to API gravity)
        voloilcalc / 0.158987294928 as oil_volume_bbl,

        -- Other properties
        volbswcalc / 0.158987294928 as bsw_volume_bbl,
        volcumcalc / 0.158987294928 as cumulative_volume_bbl,

        -- System fields
        volcumoilcalc / 0.158987294928 as cumulative_oil_volume_bbl,
        volcumbswcalc / 0.158987294928 as cumulative_bsw_volume_bbl,
        volgas / 28.316846592 as gas_volume_mcf,
        volcumgascalc / 28.316846592 as cumulative_gas_volume_mcf,
        rategas / 28.316846592 as gas_rate_mcf_per_day,
        oilratecalc / 0.1589873 as oil_rate_bbl_per_day,
        bsw / 0.01 as basic_sediments_water_percent,
        sandcut / 0.01 as sand_cut_percent,
        salinity / 1e-06 as salinity_ppm,
        h2s / 1e-06 as h2s_ppm,

        -- Fivetran fields
        power(nullif(density, 0), -1) / 7.07409872233005e-06 - 131.5 as density_api

    from source_data
)

select * from renamed
