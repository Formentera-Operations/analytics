{{ config(
    materialized='view',
    tags=['wellview', 'perforations', 'completions', 'guns', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVPERFORATION') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idwell as well_id,
        idrec as record_id,

        -- Basic perforation information
        proposedoractual as proposed_or_actual,
        dttm as perforation_date,
        typ as perforation_type,

        -- Perforation depths (converted to US units)
        shotplan as shots_planned,
        shottotal as entered_shot_total,
        shotstotalcalc as calculated_shot_total,
        shotstotalaltcalc as calculated_shot_total_alt,
        shotsmisfirecalc as shots_misfired,
        intno as stage_number,

        -- Reference depths (converted to US units)
        cluserrefno as cluster_reference_number,
        gundes as gun_description,
        gunmetallurgy as gun_metallurgy,
        guncentralize as gun_centralize,
        gunleftinhole as gun_left_in_hole,

        -- Fluid depths (converted to US units)
        conveymeth as conveyance_method,
        carrierdes as carrier_description,
        carriermake as carrier_make,
        chargetyp as charge_type,

        -- Shot information
        chargemake as charge_make,
        explosivetyp as explosive_type,
        phasing as phasing_degrees,
        orientation as orientation,
        orientmethod as orientation_method,
        balance as over_under_balanced,
        fluidtyp as fluid_type,

        -- Stage and cluster information
        presbhtyp as bh_pressure_type,
        currentstatuscalc as current_status,

        -- Gun specifications (converted to US units)
        dttmstatuscalc as current_status_date,
        statusprimarycalc as open_or_closed,
        resulttechnical as technical_result,
        resulttechnicaldetail as tech_result_details,
        resulttechnicalnote as tech_result_note,
        formationcalc as formation,

        -- Carrier information
        reservoircalc as reservoir,
        idrecwellbore as wellbore_id,

        -- Charge specifications (converted to US units)
        idrecwellboretk as wellbore_table_key,
        idreczoneor as zone_id,
        idreczoneortk as zone_table_key,
        idreczonecalc as linked_zone_id,
        idreczonecalctk as linked_zone_table_key,
        idrecstring as string_perforated_id,
        idrecstringtk as string_perforated_table_key,

        -- Hole specifications (converted to US units)
        idrecjob as job_id,
        idrecjobtk as job_table_key,
        idreclog as reference_log_id,

        -- Perforating conditions
        idreclogtk as reference_log_table_key,
        idrecjobprogramphasecalc as phase_id,
        idrecjobprogramphasecalctk as phase_table_key,
        idreclastcompletioncalc as last_completion_id,
        idreclastcompletioncalctk as last_completion_table_key,

        -- Fluid information
        idreclastrigcalc as last_rig_id,
        idreclastrigcalctk as last_rig_table_key,
        idrecotherinholecalc as other_in_hole_id,
        idrecotherinholecalctk as other_in_hole_table_key,

        -- Pressure measurements (converted to US units)
        contractor as perforation_company,
        icondrawshort as draw_short,
        com as comment,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockdate as system_lock_date,
        syscreatedate as created_at,
        syscreateuser as created_by,

        -- Status and results
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,
        _fivetran_synced as fivetran_synced_at,
        depthtop / 0.3048 as top_depth_ft,
        depthbtm / 0.3048 as bottom_depth_ft,

        -- Formation and reservoir
        depthtvdtopcalc / 0.3048 as top_depth_tvd_ft,
        depthtvdbtmcalc / 0.3048 as bottom_depth_tvd_ft,

        -- Wellbore and zone relationships
        depthtoptobtmcalc / 0.3048 as perforation_interval_thickness_ft,
        depthmppcalc / 0.3048 as depth_mpp_ft,
        depthcsngcollarref / 0.3048 as collar_ref_depth_ft,
        depthtvdcsngcollarrefcalc / 0.3048 as collar_ref_depth_tvd_ft,
        distancereftotopcalc / 0.3048 as distance_ref_to_top_ft,
        depthgauge / 0.3048 as gauge_depth_ft,

        -- String and equipment references
        depthtvdgaugecalc / 0.3048 as gauge_depth_tvd_ft,
        depthfluidbefore / 0.3048 as fluid_depth_before_shot_ft,
        depthfluidafter / 0.3048 as fluid_depth_after_shot_ft,
        depthtvdfluidbeforecalc / 0.3048 as fluid_depth_before_shot_tvd_ft,
        depthtvdfluidaftercalc / 0.3048 as fluid_depth_after_shot_tvd_ft,
        shotdensity / 3.28083989501312 as shot_density_shots_per_ft,

        -- Phase and completion references
        shotstotalaltperdensitycalc / 3.28083989501312 as shots_total_per_density_per_ft,
        szgun / 0.0254 as gun_size_inches,
        chargesz / 0.001 as charge_size_grams,
        szholeact / 0.0254 as estimated_actual_hole_diameter_inches,
        szholenom / 0.0254 as nominal_hole_diameter_inches,
        penetrationnom / 0.0254 as nominal_penetration_inches,
        balancepres / 6.894757 as over_under_pressure_psi,
        presdesignbh / 6.894757 as design_bh_pressure_psi,

        -- Operational information
        presinitsurf / 6.894757 as initial_surface_pressure_psi,
        presfinalsurf / 6.894757 as final_surface_pressure_psi,

        -- User fields (volume field converted to barrels)
        fluiddensity / 119.826428404623 as fluid_density_lb_per_gal,

        -- Comments
        ratefluidbefore / 7.3152 as fluid_rate_before_shot_ft_per_hr,

        -- System locking fields
        ratefluidafter / 7.3152 as fluid_rate_after_shot_ft_per_hr,
        presbh / 6.894757 as bottom_hole_pressure_psi,
        presdatum / 6.894757 as datum_pressure_psi,
        presmpp / 6.894757 as mpp_pressure_psi,
        presduringperf / 6.894757 as pressure_during_perforation_psi,

        -- System tracking fields
        presbhtodatumcalc / 6.894757 as pressure_bh_to_datum_psi,
        presbhtomppcalc / 6.894757 as pressure_bh_to_mpp_psi,
        preshhsurftompp / 6.894757 as estimated_hh_surf_to_mpp_psi,
        presgradientgaugetompp / 22.620593832021 as pressure_gradient_gauge_to_mpp_psi_per_ft,
        presbhsitphhcalc / 6.894757 as bh_pressure_for_sitp_hh_psi,

        -- Fivetran metadata
        usernum1 / 0.158987294928 as vol_fluid_bbl

    from source_data
)

select * from renamed
