{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'operations']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBMUDCHK') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as mud_check_id,
        trim(idrecparent)::varchar as job_report_id,
        trim(idwell)::varchar as well_id,

        -- check information
        dttm::timestamp_ntz as check_datetime,
        trim(mudtyp1)::varchar as mud_type,
        trim(mudtyp2)::varchar as fluid_category,
        trim(source)::varchar as source,
        trim(checkedby)::varchar as checked_by,
        trim(contractor)::varchar as mud_company,
        dontuse::boolean as dont_use,
        trim(dontusereason)::varchar as dont_use_reason,
        trim(com)::varchar as comments,

        -- wellbore reference
        trim(idrecwellbore)::varchar as wellbore_id,
        trim(idrecwellboretk)::varchar as wellbore_table_key,

        -- depth and location (converted from meters to feet/inches)
        {{ wv_meters_to_feet('depth') }} as depth_ft,
        {{ wv_meters_to_feet('depthtvdcalc') }} as depth_tvd_ft,
        {{ wv_meters_to_inches('wellboreszcalc') }} as wellbore_size_in,

        -- basic mud properties (converted from kg/m3 to lb/gal)
        {{ wv_kgm3_to_lb_per_gal('density') }} as density_lb_per_gal,
        {{ wv_kgm3_to_lb_per_gal('ecd') }} as ecd_lb_per_gal,

        -- temperatures (converted from celsius to fahrenheit)
        {{ wv_celsius_to_fahrenheit('tempflowline') }} as flowline_temperature_deg_f,
        {{ wv_celsius_to_fahrenheit('tempbottomhole') }} as bottom_hole_temperature_deg_f,
        {{ wv_celsius_to_fahrenheit('tempcrystal') }} as crystal_temperature_deg_f,
        {{ wv_celsius_to_fahrenheit('tempph') }} as ph_temperature_deg_f,
        {{ wv_celsius_to_fahrenheit('tempvisc') }} as viscometer_temperature_deg_f,
        {{ wv_celsius_to_fahrenheit('hthptemp') }} as hthp_temperature_deg_f,

        -- pressures (converted from kPa to PSI)
        {{ wv_kpa_to_psi('presbhstaticcalc') }} as bottom_hole_static_pressure_psi,
        {{ wv_kpa_to_psi('presvisc') }} as viscometer_pressure_psi,
        {{ wv_kpa_to_psi('hthppres') }} as hthp_pressure_psi,

        -- fluid composition percentages (proportion -> percentage)
        waterpercent / 0.01 as water_percent,
        oilpercent / 0.01 as oil_percent,
        solids / 0.01 as solids_percent,
        solidscorrected / 0.01 as corrected_solids_percent,
        salt / 0.01 as salt_percent,
        sands / 0.01 as sand_percent,
        solidslowgrav / 0.01 as low_gravity_solids_percent,
        solidshighgrav / 0.01 as high_gravity_solids_percent,
        oiloncuttings / 0.01 as oil_on_cuttings_percent,
        polymer / 0.01 as polymer_percent,

        -- oil/water ratio
        oilwaterratiocalc::float as oil_water_ratio,

        -- ph
        ph::float as ph,
        trim(phmethod)::varchar as ph_method,

        -- rheological properties - viscosity (converted from Pa*s to centipoise)
        {{ wv_pas_to_cp('plasticvis') }} as plastic_viscosity_cp,
        {{ wv_pas_to_cp('plasticviscalc') }} as plastic_viscosity_calculated_cp,

        -- funnel viscosity (s/m3 -> sec/qt: / 0.0122301876091735)
        funnelviscosity / 0.0122301876091735 as funnel_viscosity_sec_per_qt,

        -- gel strengths (Pa -> lbf/100ft2: / 0.000478802589803)
        gel10sec / 0.000478802589803 as gel_10_sec_lbf_per_100ft2,
        gel10min / 0.000478802589803 as gel_10_min_lbf_per_100ft2,
        gel30min / 0.000478802589803 as gel_30_min_lbf_per_100ft2,

        -- yield point and stress (Pa -> lbf/100ft2)
        yieldpt / 0.000478802589803 as yield_point_lbf_per_100ft2,
        yieldptcalc / 0.000478802589803 as yield_point_calculated_lbf_per_100ft2,
        yieldstresscalc / 0.000478802589803 as yield_stress_calculated_lbf_per_100ft2,

        -- power law parameters
        ncalc::float as n_calculated,
        noverride::float as n_override,
        kcalc::float as k_calculated,
        koverride::float as k_override,

        -- viscometer readings (dial readings - no conversion needed)
        vis3rpm::float as viscometer_3_rpm,
        vis6rpm::float as viscometer_6_rpm,
        vis30rpm::float as viscometer_30_rpm,
        vis60rpm::float as viscometer_60_rpm,
        vis100rpm::float as viscometer_100_rpm,
        vis200rpm::float as viscometer_200_rpm,
        vis300rpm::float as viscometer_300_rpm,
        vis600rpm::float as viscometer_600_rpm,

        -- alkalinity and filtration
        alkalinity::float as alkalinity_ml_per_ml,
        mf::float as mf_ml_per_ml,
        pm::float as pm_ml_per_ml,
        pmfiltrate::float as pm_filtrate_ml_per_ml,
        pf::float as pf_ml_per_ml,
        p1::float as p1_ml_per_ml,
        p2::float as p2_ml_per_ml,

        -- filtration properties (various unit conversions)
        filtrate / 4.8e-05 as filtrate_ml_per_30min,
        filtercake / 0.00079375 as filter_cake_32nds_in,
        hthpfiltrate / 4.8e-05 as hthp_filtrate_ml_per_30min,
        hthpfiltercake / 0.00079375 as hthp_filter_cake_32nds_in,
        volspurtloss / 1e-06 as spurt_loss_volume_ml,

        -- electrical and optical properties
        elecstability::float as electrical_stability_volts,
        staticsheen::float as static_sheen,
        oilgrease::float as oil_and_grease,
        ceccuttings::float as cec_cuttings,
        ntu::float as ntu,
        ntuout::float as ntu_out,
        trim(lcm)::varchar as lcm,
        trim(polymertyp)::varchar as polymer_type,

        -- chemical concentrations (converted to mg/L)
        calcium / 0.001 as calcium_mg_per_l,
        magnesium / 0.001 as magnesium_mg_per_l,
        chlorides / 0.001 as chlorides_mg_per_l,
        iron / 0.001 as iron_mg_per_l,
        sulfide / 0.001 as sulfide_mg_per_l,
        potassium / 0.001 as potassium_mg_per_l,

        -- chemical concentrations (converted to PPM)
        cacl / 1e-06 as calcium_chloride_ppm,
        hardnessca / 1e-06 as calcium_hardness_ppm,

        -- chemical concentrations (converted to lb/gal)
        {{ wv_kgm3_to_lb_per_gal('barite') }} as barite_lb_per_gal,
        {{ wv_kgm3_to_lb_per_gal('brine') }} as brine_lb_per_gal,
        {{ wv_kgm3_to_lb_per_gal('solidshighgravwt') }} as high_gravity_solids_weight_lb_per_gal,

        -- chemical concentrations (converted to lb/bbl)
        kcl / 2.853013476 as kcl_lb_per_bbl,
        lime / 2.853013476 as lime_lb_per_bbl,
        mbt / 2.853013476 as mbt_lb_per_bbl,
        solidslowgravwt / 2.853013476 as low_gravity_solids_weight_lb_per_bbl,
        zincoxide / 2.853013476 as zinc_oxide_lb_per_bbl,

        -- specific gravity
        solidsavggrav / 1000 as average_solids_specific_gravity,

        -- size measurements (converted to inches/microns)
        {{ wv_meters_to_inches('cuttingsszavg') }} as average_cuttings_size_in,
        filtrationsz / 1e-06 as filtration_size_microns,

        -- weight measurements (converted to pounds)
        {{ wv_kg_to_lb('weightmetalrecov') }} as weight_metal_recovered_lb,

        -- wps (water phase salinity) calculations - chlorides from salts (mg/L)
        chlorcacl2calc / 0.001 as chlorides_from_cacl2_mg_per_l,
        chlorkclcalc / 0.001 as chlorides_from_kcl_mg_per_l,
        chlornaclcalc / 0.001 as chlorides_from_nacl_mg_per_l,

        -- salt concentrations (mg/L)
        conccacl2calc / 0.001 as cacl2_concentration_mg_per_l,
        concnaclcalc / 0.001 as nacl_concentration_mg_per_l,
        conckclcalc / 0.001 as kcl_concentration_mg_per_l,

        -- weight percentages (proportion -> percentage)
        wtpctcacl2calc / 0.01 as cacl2_weight_percent,
        wtpctnaclcalc / 0.01 as nacl_weight_percent,
        wtpctkclcalc / 0.01 as kcl_weight_percent,

        -- ppm concentrations
        concppmcacl2calc / 1e-06 as cacl2_concentration_ppm,
        concppmnaclcalc / 1e-06 as nacl_concentration_ppm,
        concppmkclcalc / 1e-06 as kcl_concentration_ppm,
        concppmtotalwpscalc / 1e-06 as total_wps_concentration_ppm,

        -- analysis metadata
        trim(outofrangecalc)::varchar as out_of_range_parameters,

        -- system / audit
        trim(syscreateuser)::varchar as created_by,
        syscreatedate::timestamp_ntz as created_at,
        trim(sysmoduser)::varchar as modified_by,
        sysmoddate::timestamp_ntz as modified_at,
        trim(systag)::varchar as system_tag,
        syslockdate::timestamp_ntz as system_lock_date,
        syslockme::boolean as system_lock_me,
        syslockchildren::boolean as system_lock_children,
        syslockmeui::boolean as system_lock_me_ui,
        syslockchildrenui::boolean as system_lock_children_ui,

        -- ingestion metadata
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_tz as _fivetran_synced

    from source
),

filtered as (
    select *
    from renamed
    where
        coalesce(_fivetran_deleted, false) = false
        and mud_check_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['mud_check_id']) }} as mud_check_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        mud_check_sk,

        -- identifiers
        mud_check_id,
        job_report_id,
        well_id,

        -- check information
        check_datetime,
        mud_type,
        fluid_category,
        source,
        checked_by,
        mud_company,
        dont_use,
        dont_use_reason,
        comments,

        -- wellbore reference
        wellbore_id,
        wellbore_table_key,

        -- depth and location
        depth_ft,
        depth_tvd_ft,
        wellbore_size_in,

        -- basic mud properties
        density_lb_per_gal,
        ecd_lb_per_gal,

        -- temperatures
        flowline_temperature_deg_f,
        bottom_hole_temperature_deg_f,
        crystal_temperature_deg_f,
        ph_temperature_deg_f,
        viscometer_temperature_deg_f,
        hthp_temperature_deg_f,

        -- pressures
        bottom_hole_static_pressure_psi,
        viscometer_pressure_psi,
        hthp_pressure_psi,

        -- fluid composition percentages
        water_percent,
        oil_percent,
        solids_percent,
        corrected_solids_percent,
        salt_percent,
        sand_percent,
        low_gravity_solids_percent,
        high_gravity_solids_percent,
        oil_on_cuttings_percent,
        polymer_percent,

        -- oil/water ratio
        oil_water_ratio,

        -- ph
        ph,
        ph_method,

        -- rheological properties
        plastic_viscosity_cp,
        plastic_viscosity_calculated_cp,
        funnel_viscosity_sec_per_qt,

        -- gel strengths
        gel_10_sec_lbf_per_100ft2,
        gel_10_min_lbf_per_100ft2,
        gel_30_min_lbf_per_100ft2,

        -- yield point and stress
        yield_point_lbf_per_100ft2,
        yield_point_calculated_lbf_per_100ft2,
        yield_stress_calculated_lbf_per_100ft2,

        -- power law parameters
        n_calculated,
        n_override,
        k_calculated,
        k_override,

        -- viscometer readings
        viscometer_3_rpm,
        viscometer_6_rpm,
        viscometer_30_rpm,
        viscometer_60_rpm,
        viscometer_100_rpm,
        viscometer_200_rpm,
        viscometer_300_rpm,
        viscometer_600_rpm,

        -- alkalinity and filtration
        alkalinity_ml_per_ml,
        mf_ml_per_ml,
        pm_ml_per_ml,
        pm_filtrate_ml_per_ml,
        pf_ml_per_ml,
        p1_ml_per_ml,
        p2_ml_per_ml,

        -- filtration properties
        filtrate_ml_per_30min,
        filter_cake_32nds_in,
        hthp_filtrate_ml_per_30min,
        hthp_filter_cake_32nds_in,
        spurt_loss_volume_ml,

        -- electrical and optical properties
        electrical_stability_volts,
        static_sheen,
        oil_and_grease,
        cec_cuttings,
        ntu,
        ntu_out,
        lcm,
        polymer_type,

        -- chemical concentrations (mg/L)
        calcium_mg_per_l,
        magnesium_mg_per_l,
        chlorides_mg_per_l,
        iron_mg_per_l,
        sulfide_mg_per_l,
        potassium_mg_per_l,

        -- chemical concentrations (PPM)
        calcium_chloride_ppm,
        calcium_hardness_ppm,

        -- chemical concentrations (lb/gal)
        barite_lb_per_gal,
        brine_lb_per_gal,
        high_gravity_solids_weight_lb_per_gal,

        -- chemical concentrations (lb/bbl)
        kcl_lb_per_bbl,
        lime_lb_per_bbl,
        mbt_lb_per_bbl,
        low_gravity_solids_weight_lb_per_bbl,
        zinc_oxide_lb_per_bbl,

        -- specific gravity
        average_solids_specific_gravity,

        -- size measurements
        average_cuttings_size_in,
        filtration_size_microns,

        -- weight measurements
        weight_metal_recovered_lb,

        -- wps calculations
        chlorides_from_cacl2_mg_per_l,
        chlorides_from_kcl_mg_per_l,
        chlorides_from_nacl_mg_per_l,
        cacl2_concentration_mg_per_l,
        nacl_concentration_mg_per_l,
        kcl_concentration_mg_per_l,
        cacl2_weight_percent,
        nacl_weight_percent,
        kcl_weight_percent,
        cacl2_concentration_ppm,
        nacl_concentration_ppm,
        kcl_concentration_ppm,
        total_wps_concentration_ppm,

        -- analysis metadata
        out_of_range_parameters,

        -- system / audit
        created_by,
        created_at,
        modified_by,
        modified_at,
        system_tag,
        system_lock_date,
        system_lock_me,
        system_lock_children,
        system_lock_me_ui,
        system_lock_children_ui,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
