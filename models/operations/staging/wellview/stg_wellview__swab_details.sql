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
        tmswab / 0.0416666666666667 as swabbing_time_hours,
        
        -- Pressures (converted to PSI)
        prestub / 6.894757 as tubing_pressure_psi,
        prescas / 6.894757 as casing_pressure_psi,
        
        -- Temperature (converted to Fahrenheit)
        tempwh / 0.555555555555556 + 32 as wellhead_temperature_f,
        
        -- Depths (converted to feet)
        depthfluidlevel / 0.3048 as tagged_fluid_level_ft,
        depthpull / 0.3048 as pull_depth_ft,
        depthtvdfluidlevelcalc / 0.3048 as fluid_level_tvd_ft,
        depthtvdpullcalc / 0.3048 as pull_depth_tvd_ft,
        
        -- Tank measurements (converted to inches)
        tankgauge / 0.0254 as tank_gauge_inches,
        
        -- Fluid volumes (converted to barrels)
        volfluidrec / 0.158987294928 as recovered_fluid_volume_bbl,
        voloilcalc / 0.158987294928 as oil_volume_bbl,
        volbswcalc / 0.158987294928 as bsw_volume_bbl,
        volcumcalc / 0.158987294928 as cumulative_volume_bbl,
        volcumoilcalc / 0.158987294928 as cumulative_oil_volume_bbl,
        volcumbswcalc / 0.158987294928 as cumulative_bsw_volume_bbl,
        
        -- Gas volumes and rates (converted to MCF and MCF/day)
        volgas / 28.316846592 as gas_volume_mcf,
        volcumgascalc / 28.316846592 as cumulative_gas_volume_mcf,
        rategas / 28.316846592 as gas_rate_mcf_per_day,
        
        -- Oil rate (converted to BBL/day)
        oilratecalc / 0.1589873 as oil_rate_bbl_per_day,
        
        -- Fluid properties (converted to percentages and API)
        bsw / 0.01 as basic_sediments_water_percent,
        sandcut / 0.01 as sand_cut_percent,
        salinity / 1e-06 as salinity_ppm,
        h2s / 1e-06 as h2s_ppm,
        
        -- Density (converted to API gravity)
        power(nullif(density, 0), -1) / 7.07409872233005e-06 - 131.5 as density_api,
        
        -- Other properties
        ph as ph_value,
        com as comments,
        
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