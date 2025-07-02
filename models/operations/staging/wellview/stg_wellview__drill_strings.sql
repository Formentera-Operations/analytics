{{ config(
    materialized='view',
    tags=['wellview', 'drilling', 'drillstring', 'bha', 'performance', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBDRILLSTRING') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as drill_string_id,
        idrecparent as job_id,
        idwell as well_id,
        
        -- Basic drill string information
        proposedoractual as proposed_or_actual,
        des as drill_string_description,
        stringno as bha_number,
        propversionno as proposed_version_number,
        
        -- Bit and BHA information
        idrecbit as drill_bit_id,
        idrecbittk as drill_bit_table_key,
        bitno as bit_run_number,
        bitnozzlecalc as bit_nozzles,
        bitnozzlealtcalc as bit_nozzles_alt_format,
        bittfa / 0.00064516 as bit_total_fluid_area_sq_in,
        bittfacalc / 0.00064516 as bit_total_fluid_area_calc_sq_in,
        bitwearcalc as iadc_bit_dull_code,
        
        -- String objectives and results
        stringobjective as bha_objective,
        dircontrolmethod as directional_control_method,
        complexityindex as complexity_index,
        stringresult as bha_result,
        stringresultdetail as bha_result_details,
        stringresultnote as bha_result_notes,
        
        -- Bit dull grading
        wearinner as bit_dull_tooth_inner,
        wearouter as bit_dull_tooth_outer,
        weardull as bit_dull_tooth_dull_char,
        wearloc as bit_dull_tooth_location,
        wearbearing as bit_dull_bearing,
        weargauge as bit_dull_gauge,
        wearother as bit_dull_other,
        wearpulled as bit_dull_reason_pulled,
        
        -- Operational dates
        dttmincalc as date_in,
        dttmoutcalc as date_out,
        dttminnoexcludecalc as date_in_no_excluded_params,
        dttmoutnoexcludecalc as date_out_no_excluded_params,
        dttmpickup as date_pickup,
        dttmoutofhole as date_out_of_hole,
        
        -- Proposed dates and depths
        dttminprop as proposed_date_in,
        dttmoutprop as proposed_date_out,
        depthinprop / 0.3048 as proposed_depth_in_ft,
        depthoutprop / 0.3048 as proposed_depth_out_ft,
        
        -- Depth performance (converted to US units)
        depthincalc / 0.3048 as depth_in_ft,
        depthoutcalc / 0.3048 as depth_out_ft,
        depthinnoexcludecalc / 0.3048 as depth_in_no_excluded_ft,
        depthoutnoexcludecalc / 0.3048 as depth_out_no_excluded_ft,
        depthdrilledcalc / 0.3048 as depth_drilled_ft,
        depthdrillednoexccalc / 0.3048 as depth_drilled_no_excluded_ft,
        depthrotatingcalc / 0.3048 as depth_rotating_ft,
        depthslidingcalc / 0.3048 as depth_sliding_ft,
        
        -- Cumulative bit performance (converted to US units)
        cumdepthonbitstartcalc / 0.3048 as cum_depth_on_bit_start_ft,
        cumdepthonbitendcalc / 0.3048 as cum_depth_on_bit_end_ft,
        cumdepthonbitstartnoexccalc / 0.3048 as cum_depth_on_bit_start_no_excluded_ft,
        cumdepthonbitendnoexccalc / 0.3048 as cum_depth_on_bit_end_no_excluded_ft,
        cumtmonbitstartcalc / 0.0416666666666667 as cum_time_on_bit_start_hr,
        cumtmonbitendcalc / 0.0416666666666667 as cum_time_on_bit_end_hr,
        cumtmonbitstartnoexccalc / 0.0416666666666667 as cum_time_on_bit_start_no_excluded_hr,
        cumtmonbitendnoexccalc / 0.0416666666666667 as cum_time_on_bit_end_no_excluded_hr,
        
        -- Time performance (converted to US units)
        tmdrilledcalc / 0.0416666666666667 as drilling_time_hr,
        tmdrillnoexccalc / 0.0416666666666667 as drilling_time_no_excluded_hr,
        tmcirccalc / 0.0416666666666667 as circulating_time_hr,
        tmtripcalc / 0.0416666666666667 as tripping_time_hr,
        tmothercalc / 0.0416666666666667 as other_time_hr,
        tmcirctripothercalc / 0.0416666666666667 as circ_trip_other_time_hr,
        tmrotatingcalc / 0.0416666666666667 as rotating_time_hr,
        tmslidingcalc / 0.0416666666666667 as sliding_time_hr,
        
        -- Duration calculations (converted to US units)
        duroutofholetopickupcalc / 0.0416666666666667 as out_of_hole_to_pickup_duration_hr,
        duroffbtmcalc / 0.000694444444444444 as off_bottom_duration_min,
        duronbtmcalc / 0.000694444444444444 as on_bottom_duration_min,
        durpipemovingcalc / 0.000694444444444444 as pipe_moving_duration_min,
        
        -- Rate of penetration (converted to US units)
        ropcalc / 7.3152 as avg_rop_ft_per_hr,
        ropnoexccalc / 7.3152 as avg_rop_no_excluded_ft_per_hr,
        ropinstavgcalc / 7.3152 as avg_instantaneous_rop_ft_per_hr,
        roprotatingcalc / 7.3152 as rotating_rop_ft_per_hr,
        ropslidingcalc / 7.3152 as sliding_rop_ft_per_hr,
        
        -- Drilling parameters - percentages
        percentdepthrotatingcalc / 0.01 as percent_depth_rotating,
        percentdepthslidingcalc / 0.01 as percent_depth_sliding,
        percenttmrotatingcalc / 0.01 as percent_time_rotating,
        percenttmslidingcalc / 0.01 as percent_time_sliding,
        
        -- Bit revolutions
        bitrevscalc as bit_revolutions,
        bitrevsnoexccalc as bit_revolutions_no_excluded,
        
        -- RPM parameters
        rpmavgcalc as avg_rpm,
        rpmmincalc as min_rpm,
        rpmmaxcalc as max_rpm,
        rpmavgnoexccalc as avg_rpm_no_excluded,
        rpmmincalc as min_rpm_no_excluded,
        rpmmaxnoexccalc as max_rpm_no_excluded,
        
        -- Weight on bit (converted to US units)
        wobavgcalc / 4448.2216152605 as avg_weight_on_bit_klbf,
        wobmincalc / 4448.2216152605 as min_weight_on_bit_klbf,
        wobmaxcalc / 4448.2216152605 as max_weight_on_bit_klbf,
        wobavgnoexccalc / 4448.2216152605 as avg_weight_on_bit_no_excluded_klbf,
        wobminnoexccalc / 4448.2216152605 as min_weight_on_bit_no_excluded_klbf,
        wobmaxnoexccalc / 4448.2216152605 as max_weight_on_bit_no_excluded_klbf,
        
        -- Hook load parameters (converted to US units)
        hookloadrotatingmincalc / 4448.2216152605 as min_rotating_hook_load_klbf,
        hookloadrotatingmaxcalc / 4448.2216152605 as max_rotating_hook_load_klbf,
        hookloadrotatingminnoexccalc / 4448.2216152605 as min_rotating_hook_load_no_excluded_klbf,
        hookloadrotatingmaxnoexccalc / 4448.2216152605 as max_rotating_hook_load_no_excluded_klbf,
        hookloadoffbottommincalc / 4448.2216152605 as min_off_bottom_hook_load_klbf,
        hookloadoffbottommaxcalc / 4448.2216152605 as max_off_bottom_hook_load_klbf,
        hookloadoffbottomminnoexccalc / 4448.2216152605 as min_off_bottom_hook_load_no_excluded_klbf,
        hookloadoffbottommaxnoexccalc / 4448.2216152605 as max_off_bottom_hook_load_no_excluded_klbf,
        hookloadpickupmincalc / 4448.2216152605 as min_pickup_hook_load_klbf,
        hookloadpickupmaxcalc / 4448.2216152605 as max_pickup_hook_load_klbf,
        hookloadpickupminnoexccalc / 4448.2216152605 as min_pickup_hook_load_no_excluded_klbf,
        hookloadpickupmaxnoexccalc / 4448.2216152605 as max_pickup_hook_load_no_excluded_klbf,
        hookloadslackoffmincalc / 4448.2216152605 as min_slackoff_hook_load_klbf,
        hookloadslackoffmaxcalc / 4448.2216152605 as max_slackoff_hook_load_klbf,
        hookloadslackoffminnoexccalc / 4448.2216152605 as min_slackoff_hook_load_no_excluded_klbf,
        hookloadslackoffmaxnoexccalc / 4448.2216152605 as max_slackoff_hook_load_no_excluded_klbf,
        
        -- Flow rate parameters (converted to US units)
        liquidinjrateavgcalc / 5.45099328 as avg_flow_rate_gpm,
        liquidinjratemincalc / 5.45099328 as min_flow_rate_gpm,
        liquidinjratemaxcalc / 5.45099328 as max_flow_rate_gpm,
        liquidinjrateriseravgcalc / 5.45099328 as avg_riser_boost_gpm,
        liquidinjraterisermincalc / 5.45099328 as min_riser_boost_gpm,
        liquidinjraterisermaxcalc / 5.45099328 as max_riser_boost_gpm,
        
        -- Pressure parameters (converted to US units)
        sppdrillavgcalc / 6.894757 as avg_standpipe_pressure_psi,
        sppdrillmincalc / 6.894757 as min_standpipe_pressure_psi,
        sppdrillmaxcalc / 6.894757 as max_standpipe_pressure_psi,
        sppdrillavgnoexccalc / 6.894757 as avg_standpipe_pressure_no_excluded_psi,
        sppdrillminnoexccalc / 6.894757 as min_standpipe_pressure_no_excluded_psi,
        sppdrillmaxnoexccalc / 6.894757 as max_standpipe_pressure_no_excluded_psi,
        
        -- Torque parameters (no conversion - units unclear)
        torquedrillavgcalc as avg_drilling_torque,
        torquedrillmincalc as min_drilling_torque,
        torquedrillmaxcalc as max_drilling_torque,
        
        -- Mud properties (converted to US units)
        densitylastmudchkavgcalc / 119.826428404623 as avg_last_mud_density_ppg,
        densitylastmudchkmincalc / 119.826428404623 as min_last_mud_density_ppg,
        densitylastmudchkmaxcalc / 119.826428404623 as max_last_mud_density_ppg,
        muddensitymincalc / 119.826428404623 as min_mud_density_ppg,
        muddensitymaxcalc / 119.826428404623 as max_mud_density_ppg,
        
        -- ECD parameters (converted to US units)
        ecdendavgcalc / 119.826428404623 as avg_ecd_end_ppg,
        ecdendmincalc / 119.826428404623 as min_ecd_end_ppg,
        ecdendmaxcalc / 119.826428404623 as max_ecd_end_ppg,
        
        -- Hydraulic power (converted to US units)
        hydpwrperareaavgcalc / 1155837.15667431 as avg_hydraulic_power_per_area_hp_per_sq_in,
        hydpwrperareamincalc / 1155837.15667431 as min_hydraulic_power_per_area_hp_per_sq_in,
        hydpwrperareamaxcalc / 1155837.15667431 as max_hydraulic_power_per_area_hp_per_sq_in,
        
        -- String dimensions (converted to US units)
        lengthcalc / 0.3048 as string_length_ft,
        lengthbittohocalc / 0.3048 as bit_to_hole_opener_length_ft,
        szdriftmincalc / 0.0254 as min_drift_in,
        szodmaxcalc / 0.0254 as max_nominal_od_in,
        szodhocalc / 0.0254 as hole_opener_od_in,
        
        -- String weight and volumes (converted to US units)
        weightaircalc / 4448.2216152605 as string_weight_in_air_klbf,
        weightairnodpcalc / 4448.2216152605 as string_weight_in_air_no_drill_pipe_klbf,
        volumedispcalc / 0.158987294928 as volume_displaced_bbl,
        volumeinternalcalc / 0.158987294928 as string_internal_volume_bbl,
        
        -- Tripping parameters (converted to US units)
        travelequipwt / 4448.2216152605 as traveling_equipment_weight_klbf,
        triphookloadmax / 4448.2216152605 as max_trip_hook_load_klbf,
        tripoverpullmax / 4448.2216152605 as max_trip_over_pull_klbf,
        triprateinavg / 7.3152 as avg_trip_rate_in_ft_per_hr,
        triprateinmax / 7.3152 as max_trip_rate_in_ft_per_hr,
        triprateoutavg / 7.3152 as avg_trip_rate_out_ft_per_hr,
        triprateoutmax / 7.3152 as max_trip_rate_out_ft_per_hr,
        tripnote as trip_note,
        
        -- Inclination data (converted to US units)
        inclstringtopcalc as top_inclination_deg,
        inclstringbtmcalc as bottom_inclination_deg,
        inclstringmaxcalc as max_inclination_deg,
        
        -- Equipment flags
        centralizersnotallycalc as number_of_centralizers_tally,
        mudmotorcalc as mud_motor_present,
        vgscalc as vgs_present,
        qtyplugcalc as quantity_plugs_drilled,
        
        -- String components and descriptions
        componentscalc as string_components,
        componentsdimcalc as string_component_dimensions,
        componentsaltdimcalc as string_component_dimensions_alt,
        
        -- Related entities
        idrecwellborecalc as wellbore_id,
        idrecwellborecalctk as wellbore_table_key,
        idreclastrigcalc as last_rig_id,
        idreclastrigcalctk as last_rig_table_key,
        
        -- Comments and notes
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