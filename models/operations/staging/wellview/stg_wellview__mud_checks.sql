{{ config(
    materialized='view',
    tags=['wellview', 'drilling', 'mud', 'checks', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBMUDCHK') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as mud_check_id,
        idrecparent as job_report_id,
        idwell as well_id,
        
        -- Check information
        dttm as check_datetime,
        mudtyp1 as mud_type,
        mudtyp2 as fluid_category,
        source as source,
        checkedby as checked_by,
        contractor as mud_company,
        dontuse as dont_use,
        dontusereason as dont_use_reason,
        com as comments,
        
        -- Wellbore reference
        idrecwellbore as wellbore_id,
        idrecwellboretk as wellbore_table_key,
        
        -- Depth and location (converted to US units - feet)
        depth / 0.3048 as depth_ft,
        depthtvdcalc / 0.3048 as depth_tvd_ft,
        wellboreszcalc / 0.0254 as wellbore_size_in,
        
        -- Basic mud properties (converted to US units)
        density / 119.826428404623 as density_lb_per_gal,
        ecd / 119.826428404623 as ecd_lb_per_gal,
        
        -- Temperatures (converted to US units - Fahrenheit)
        tempflowline / 0.555555555555556 + 32 as flowline_temperature_deg_f,
        tempbottomhole / 0.555555555555556 + 32 as bottom_hole_temperature_deg_f,
        tempcrystal / 0.555555555555556 + 32 as crystal_temperature_deg_f,
        tempph / 0.555555555555556 + 32 as ph_temperature_deg_f,
        tempvisc / 0.555555555555556 + 32 as viscometer_temperature_deg_f,
        
        -- Pressures (converted to US units - PSI)
        presbhstaticcalc / 6.894757 as bottom_hole_static_pressure_psi,
        presvisc / 6.894757 as viscometer_pressure_psi,
        hthppres / 6.894757 as hthp_pressure_psi,
        hthptemp / 0.555555555555556 + 32 as hthp_temperature_deg_f,
        
        -- Fluid composition percentages (converted to %)
        waterpercent / 0.01 as water_percent,
        oilpercent / 0.01 as oil_percent,
        oilwaterratiocalc as oil_water_ratio,
        solids / 0.01 as solids_percent,
        solidscorrected / 0.01 as corrected_solids_percent,
        salt / 0.01 as salt_percent,
        sands / 0.01 as sand_percent,
        solidslowgrav / 0.01 as low_gravity_solids_percent,
        solidshighgrav / 0.01 as high_gravity_solids_percent,
        oiloncuttings / 0.01 as oil_on_cuttings_percent,
        polymer / 0.01 as polymer_percent,
        
        -- Rheological properties
        ph as ph,
        phmethod as ph_method,
        
        -- Viscosity (converted to US units - centipoise)
        plasticvis / 0.001 as plastic_viscosity_cp,
        plasticviscalc / 0.001 as plastic_viscosity_calculated_cp,
        funnelviscosity / 0.0122301876091735 as funnel_viscosity_sec_per_qt,
        
        -- Gel strengths (converted to US units - lbf/100ft²)
        gel10sec / 0.000478802589803 as gel_10_sec_lbf_per_100ft2,
        gel10min / 0.000478802589803 as gel_10_min_lbf_per_100ft2,
        gel30min / 0.000478802589803 as gel_30_min_lbf_per_100ft2,
        
        -- Yield point and stress (converted to US units - lbf/100ft²)
        yieldpt / 0.000478802589803 as yield_point_lbf_per_100ft2,
        yieldptcalc / 0.000478802589803 as yield_point_calculated_lbf_per_100ft2,
        yieldstresscalc / 0.000478802589803 as yield_stress_calculated_lbf_per_100ft2,
        
        -- Power law parameters
        ncalc as n_calculated,
        noverride as n_override,
        kcalc as k_calculated,
        koverride as k_override,
        
        -- Viscometer readings (dial readings - no conversion needed)
        vis3rpm as viscometer_3_rpm,
        vis6rpm as viscometer_6_rpm,
        vis30rpm as viscometer_30_rpm,
        vis60rpm as viscometer_60_rpm,
        vis100rpm as viscometer_100_rpm,
        vis200rpm as viscometer_200_rpm,
        vis300rpm as viscometer_300_rpm,
        vis600rpm as viscometer_600_rpm,
        
        -- Filtration properties
        filtrate / 4.8e-05 as filtrate_ml_per_30min,
        filtercake / 0.00079375 as filter_cake_32nds_in,
        hthpfiltrate / 4.8e-05 as hthp_filtrate_ml_per_30min,
        hthpfiltercake / 0.00079375 as hthp_filter_cake_32nds_in,
        volspurtloss / 1e-06 as spurt_loss_volume_ml,
        
        -- Chemical concentrations (converted to mg/L)
        calcium / 0.001 as calcium_mg_per_l,
        magnesium / 0.001 as magnesium_mg_per_l,
        chlorides / 0.001 as chlorides_mg_per_l,
        iron / 0.001 as iron_mg_per_l,
        sulfide / 0.001 as sulfide_mg_per_l,
        potassium / 0.001 as potassium_mg_per_l,
        
        -- Chemical concentrations (converted to PPM)
        cacl / 1e-06 as calcium_chloride_ppm,
        hardnessca / 1e-06 as calcium_hardness_ppm,
        
        -- Chemical concentrations (converted to lb/gal)
        barite / 119.826428404623 as barite_lb_per_gal,
        brine / 119.826428404623 as brine_lb_per_gal,
        solidshighgravwt / 119.826428404623 as high_gravity_solids_weight_lb_per_gal,
        
        -- Chemical concentrations (converted to lb/bbl)
        kcl / 2.853013476 as kcl_lb_per_bbl,
        lime / 2.853013476 as lime_lb_per_bbl,
        mbt / 2.853013476 as mbt_lb_per_bbl,
        solidslowgravwt / 2.853013476 as low_gravity_solids_weight_lb_per_bbl,
        zincoxide / 2.853013476 as zinc_oxide_lb_per_bbl,
        
        -- Specific gravity
        solidsavggrav / 1000 as average_solids_specific_gravity,
        
        -- Other properties (no conversion needed)
        alkalinity as alkalinity_ml_per_ml,
        mf as mf_ml_per_ml,
        pm as pm_ml_per_ml,
        pmfiltrate as pm_filtrate_ml_per_ml,
        pf as pf_ml_per_ml,
        p1 as p1_ml_per_ml,
        p2 as p2_ml_per_ml,
        elecstability as electrical_stability_volts,
        staticsheen as static_sheen,
        oilgrease as oil_and_grease,
        ceccuttings as cec_cuttings,
        ntu as ntu,
        ntuout as ntu_out,
        lcm as lcm,
        polymertyp as polymer_type,
        
        -- Size measurements (converted to inches/microns)
        cuttingsszavg / 0.0254 as average_cuttings_size_in,
        filtrationsz / 1e-06 as filtration_size_microns,
        
        -- Weight measurements (converted to pounds)
        weightmetalrecov / 0.45359237 as weight_metal_recovered_lb,
        
        -- WPS (Water Phase Salinity) calculations
        -- Chlorides from salts (converted to mg/L)
        chlorcacl2calc / 0.001 as chlorides_from_cacl2_mg_per_l,
        chlorkclcalc / 0.001 as chlorides_from_kcl_mg_per_l,
        chlornaclcalc / 0.001 as chlorides_from_nacl_mg_per_l,
        
        -- Salt concentrations (converted to mg/L)
        conccacl2calc / 0.001 as cacl2_concentration_mg_per_l,
        concnaclcalc / 0.001 as nacl_concentration_mg_per_l,
        conckclcalc / 0.001 as kcl_concentration_mg_per_l,
        
        -- Weight percentages (converted to %)
        wtpctcacl2calc / 0.01 as cacl2_weight_percent,
        wtpctnaclcalc / 0.01 as nacl_weight_percent,
        wtpctkclcalc / 0.01 as kcl_weight_percent,
        
        -- PPM concentrations
        concppmcacl2calc / 1e-06 as cacl2_concentration_ppm,
        concppmnaclcalc / 1e-06 as nacl_concentration_ppm,
        concppmkclcalc / 1e-06 as kcl_concentration_ppm,
        concppmtotalwpscalc / 1e-06 as total_wps_concentration_ppm,
        
        -- Analysis flags and metadata
        outofrangecalc as out_of_range_parameters,
        
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