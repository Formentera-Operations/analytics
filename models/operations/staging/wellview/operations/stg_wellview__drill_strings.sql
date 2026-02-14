{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'operations']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBDRILLSTRING') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as drill_string_id,
        trim(idrecparent)::varchar as job_id,
        trim(idwell)::varchar as well_id,

        -- basic drill string information
        trim(proposedoractual)::varchar as proposed_or_actual,
        trim(des)::varchar as drill_string_description,
        stringno::int as bha_number,
        propversionno::int as proposed_version_number,

        -- bit and bha information
        trim(idrecbit)::varchar as drill_bit_id,
        trim(idrecbittk)::varchar as drill_bit_table_key,
        bitno::int as bit_run_number,
        trim(bitnozzlecalc)::varchar as bit_nozzles,
        trim(bitnozzlealtcalc)::varchar as bit_nozzles_alt_format,
        trim(bitwearcalc)::varchar as iadc_bit_dull_code,
        trim(stringobjective)::varchar as bha_objective,
        trim(dircontrolmethod)::varchar as directional_control_method,
        trim(complexityindex)::varchar as complexity_index,

        -- string results
        trim(stringresult)::varchar as bha_result,
        trim(stringresultdetail)::varchar as bha_result_details,
        trim(stringresultnote)::varchar as bha_result_notes,

        -- bit dull grading
        trim(wearinner)::varchar as bit_dull_tooth_inner,
        trim(wearouter)::varchar as bit_dull_tooth_outer,
        trim(weardull)::varchar as bit_dull_tooth_dull_char,
        trim(wearloc)::varchar as bit_dull_tooth_location,
        trim(wearbearing)::varchar as bit_dull_bearing,
        trim(weargauge)::varchar as bit_dull_gauge,
        trim(wearother)::varchar as bit_dull_other,
        trim(wearpulled)::varchar as bit_dull_reason_pulled,

        -- operational dates
        dttmincalc::timestamp_ntz as date_in,
        dttmoutcalc::timestamp_ntz as date_out,
        dttminnoexcludecalc::timestamp_ntz as date_in_no_excluded_params,
        dttmoutnoexcludecalc::timestamp_ntz as date_out_no_excluded_params,
        dttmpickup::timestamp_ntz as date_pickup,
        dttmoutofhole::timestamp_ntz as date_out_of_hole,
        dttminprop::timestamp_ntz as proposed_date_in,
        dttmoutprop::timestamp_ntz as proposed_date_out,

        -- bit total fluid area (m2 -> sq in: / 0.00064516)
        bittfa / 0.00064516 as bit_total_fluid_area_sq_in,
        bittfacalc / 0.00064516 as bit_total_fluid_area_calc_sq_in,

        -- depth performance (converted from meters to feet)
        {{ wv_meters_to_feet('depthinprop') }} as proposed_depth_in_ft,
        {{ wv_meters_to_feet('depthoutprop') }} as proposed_depth_out_ft,
        {{ wv_meters_to_feet('depthincalc') }} as depth_in_ft,
        {{ wv_meters_to_feet('depthoutcalc') }} as depth_out_ft,
        {{ wv_meters_to_feet('depthinnoexcludecalc') }} as depth_in_no_excluded_ft,
        {{ wv_meters_to_feet('depthoutnoexcludecalc') }} as depth_out_no_excluded_ft,
        {{ wv_meters_to_feet('depthdrilledcalc') }} as depth_drilled_ft,
        {{ wv_meters_to_feet('depthdrillednoexccalc') }} as depth_drilled_no_excluded_ft,
        {{ wv_meters_to_feet('depthrotatingcalc') }} as depth_rotating_ft,
        {{ wv_meters_to_feet('depthslidingcalc') }} as depth_sliding_ft,

        -- cumulative bit depth (converted from meters to feet)
        {{ wv_meters_to_feet('cumdepthonbitstartcalc') }} as cum_depth_on_bit_start_ft,
        {{ wv_meters_to_feet('cumdepthonbitendcalc') }} as cum_depth_on_bit_end_ft,
        {{ wv_meters_to_feet('cumdepthonbitstartnoexccalc') }} as cum_depth_on_bit_start_no_excluded_ft,
        {{ wv_meters_to_feet('cumdepthonbitendnoexccalc') }} as cum_depth_on_bit_end_no_excluded_ft,

        -- time performance (converted from days to hours)
        {{ wv_days_to_hours('cumtmonbitstartcalc') }} as cum_time_on_bit_start_hr,
        {{ wv_days_to_hours('cumtmonbitendcalc') }} as cum_time_on_bit_end_hr,
        {{ wv_days_to_hours('cumtmonbitstartnoexccalc') }} as cum_time_on_bit_start_no_excluded_hr,
        {{ wv_days_to_hours('cumtmonbitendnoexccalc') }} as cum_time_on_bit_end_no_excluded_hr,
        {{ wv_days_to_hours('tmdrilledcalc') }} as drilling_time_hr,
        {{ wv_days_to_hours('tmdrillnoexccalc') }} as drilling_time_no_excluded_hr,
        {{ wv_days_to_hours('tmcirccalc') }} as circulating_time_hr,
        {{ wv_days_to_hours('tmtripcalc') }} as tripping_time_hr,
        {{ wv_days_to_hours('tmothercalc') }} as other_time_hr,
        {{ wv_days_to_hours('tmcirctripothercalc') }} as circ_trip_other_time_hr,
        {{ wv_days_to_hours('tmrotatingcalc') }} as rotating_time_hr,
        {{ wv_days_to_hours('tmslidingcalc') }} as sliding_time_hr,
        {{ wv_days_to_hours('duroutofholetopickupcalc') }} as out_of_hole_to_pickup_duration_hr,

        -- duration in minutes (converted from days to minutes)
        {{ wv_days_to_minutes('duroffbtmcalc') }} as off_bottom_duration_min,
        {{ wv_days_to_minutes('duronbtmcalc') }} as on_bottom_duration_min,
        {{ wv_days_to_minutes('durpipemovingcalc') }} as pipe_moving_duration_min,

        -- rate of penetration (converted from m/s to ft/hr)
        {{ wv_mps_to_ft_per_hr('ropcalc') }} as avg_rop_ft_per_hr,
        {{ wv_mps_to_ft_per_hr('ropnoexccalc') }} as avg_rop_no_excluded_ft_per_hr,
        {{ wv_mps_to_ft_per_hr('ropinstavgcalc') }} as avg_instantaneous_rop_ft_per_hr,
        {{ wv_mps_to_ft_per_hr('roprotatingcalc') }} as rotating_rop_ft_per_hr,
        {{ wv_mps_to_ft_per_hr('ropslidingcalc') }} as sliding_rop_ft_per_hr,

        -- drilling percentages (proportion -> percentage)
        percentdepthrotatingcalc / 0.01 as percent_depth_rotating,
        percentdepthslidingcalc / 0.01 as percent_depth_sliding,
        percenttmrotatingcalc / 0.01 as percent_time_rotating,
        percenttmslidingcalc / 0.01 as percent_time_sliding,

        -- rpm parameters
        bitrevscalc::float as bit_revolutions,
        bitrevsnoexccalc::float as bit_revolutions_no_excluded,
        rpmavgcalc::float as avg_rpm,
        rpmmincalc::float as min_rpm,
        rpmmaxcalc::float as max_rpm,
        rpmavgnoexccalc::float as avg_rpm_no_excluded,
        rpmmaxnoexccalc::float as max_rpm_no_excluded,

        -- torque parameters
        torquedrillavgcalc::float as avg_drilling_torque,
        torquedrillmincalc::float as min_drilling_torque,
        torquedrillmaxcalc::float as max_drilling_torque,

        -- weight on bit (converted from newtons to klbf)
        {{ wv_newtons_to_klbf('wobavgcalc') }} as avg_weight_on_bit_klbf,
        {{ wv_newtons_to_klbf('wobmincalc') }} as min_weight_on_bit_klbf,
        {{ wv_newtons_to_klbf('wobmaxcalc') }} as max_weight_on_bit_klbf,
        {{ wv_newtons_to_klbf('wobavgnoexccalc') }} as avg_weight_on_bit_no_excluded_klbf,
        {{ wv_newtons_to_klbf('wobminnoexccalc') }} as min_weight_on_bit_no_excluded_klbf,
        {{ wv_newtons_to_klbf('wobmaxnoexccalc') }} as max_weight_on_bit_no_excluded_klbf,

        -- hook load parameters (converted from newtons to klbf)
        {{ wv_newtons_to_klbf('hookloadrotatingmincalc') }} as min_rotating_hook_load_klbf,
        {{ wv_newtons_to_klbf('hookloadrotatingmaxcalc') }} as max_rotating_hook_load_klbf,
        {{ wv_newtons_to_klbf('hookloadrotatingminnoexccalc') }} as min_rotating_hook_load_no_excluded_klbf,
        {{ wv_newtons_to_klbf('hookloadrotatingmaxnoexccalc') }} as max_rotating_hook_load_no_excluded_klbf,
        {{ wv_newtons_to_klbf('hookloadoffbottommincalc') }} as min_off_bottom_hook_load_klbf,
        {{ wv_newtons_to_klbf('hookloadoffbottommaxcalc') }} as max_off_bottom_hook_load_klbf,
        {{ wv_newtons_to_klbf('hookloadoffbottomminnoexccalc') }} as min_off_bottom_hook_load_no_excluded_klbf,
        {{ wv_newtons_to_klbf('hookloadoffbottommaxnoexccalc') }} as max_off_bottom_hook_load_no_excluded_klbf,
        {{ wv_newtons_to_klbf('hookloadpickupmincalc') }} as min_pickup_hook_load_klbf,
        {{ wv_newtons_to_klbf('hookloadpickupmaxcalc') }} as max_pickup_hook_load_klbf,
        {{ wv_newtons_to_klbf('hookloadpickupminnoexccalc') }} as min_pickup_hook_load_no_excluded_klbf,
        {{ wv_newtons_to_klbf('hookloadpickupmaxnoexccalc') }} as max_pickup_hook_load_no_excluded_klbf,
        {{ wv_newtons_to_klbf('hookloadslackoffmincalc') }} as min_slackoff_hook_load_klbf,
        {{ wv_newtons_to_klbf('hookloadslackoffmaxcalc') }} as max_slackoff_hook_load_klbf,
        {{ wv_newtons_to_klbf('hookloadslackoffminnoexccalc') }} as min_slackoff_hook_load_no_excluded_klbf,
        {{ wv_newtons_to_klbf('hookloadslackoffmaxnoexccalc') }} as max_slackoff_hook_load_no_excluded_klbf,

        -- flow rate parameters (converted from m3/s to gpm)
        {{ wv_cbm_per_sec_to_gpm('liquidinjrateavgcalc') }} as avg_flow_rate_gpm,
        {{ wv_cbm_per_sec_to_gpm('liquidinjratemincalc') }} as min_flow_rate_gpm,
        {{ wv_cbm_per_sec_to_gpm('liquidinjratemaxcalc') }} as max_flow_rate_gpm,
        {{ wv_cbm_per_sec_to_gpm('liquidinjrateriseravgcalc') }} as avg_riser_boost_gpm,
        {{ wv_cbm_per_sec_to_gpm('liquidinjraterisermincalc') }} as min_riser_boost_gpm,
        {{ wv_cbm_per_sec_to_gpm('liquidinjraterisermaxcalc') }} as max_riser_boost_gpm,

        -- pressure parameters (converted from kPa to PSI)
        {{ wv_kpa_to_psi('sppdrillavgcalc') }} as avg_standpipe_pressure_psi,
        {{ wv_kpa_to_psi('sppdrillmincalc') }} as min_standpipe_pressure_psi,
        {{ wv_kpa_to_psi('sppdrillmaxcalc') }} as max_standpipe_pressure_psi,
        {{ wv_kpa_to_psi('sppdrillavgnoexccalc') }} as avg_standpipe_pressure_no_excluded_psi,
        {{ wv_kpa_to_psi('sppdrillminnoexccalc') }} as min_standpipe_pressure_no_excluded_psi,
        {{ wv_kpa_to_psi('sppdrillmaxnoexccalc') }} as max_standpipe_pressure_no_excluded_psi,

        -- mud density (converted from kg/m3 to lb/gal)
        {{ wv_kgm3_to_lb_per_gal('densitylastmudchkavgcalc') }} as avg_last_mud_density_ppg,
        {{ wv_kgm3_to_lb_per_gal('densitylastmudchkmincalc') }} as min_last_mud_density_ppg,
        {{ wv_kgm3_to_lb_per_gal('densitylastmudchkmaxcalc') }} as max_last_mud_density_ppg,
        {{ wv_kgm3_to_lb_per_gal('muddensitymincalc') }} as min_mud_density_ppg,
        {{ wv_kgm3_to_lb_per_gal('muddensitymaxcalc') }} as max_mud_density_ppg,

        -- ecd parameters (converted from kg/m3 to lb/gal)
        {{ wv_kgm3_to_lb_per_gal('ecdendavgcalc') }} as avg_ecd_end_ppg,
        {{ wv_kgm3_to_lb_per_gal('ecdendmincalc') }} as min_ecd_end_ppg,
        {{ wv_kgm3_to_lb_per_gal('ecdendmaxcalc') }} as max_ecd_end_ppg,

        -- hydraulic power (watts/m2 -> hp/in2: / 1155837.15667431)
        hydpwrperareaavgcalc / 1155837.15667431 as avg_hydraulic_power_per_area_hp_per_sq_in,
        hydpwrperareamincalc / 1155837.15667431 as min_hydraulic_power_per_area_hp_per_sq_in,
        hydpwrperareamaxcalc / 1155837.15667431 as max_hydraulic_power_per_area_hp_per_sq_in,

        -- string dimensions (converted from meters)
        {{ wv_meters_to_feet('lengthcalc') }} as string_length_ft,
        {{ wv_meters_to_feet('lengthbittohocalc') }} as bit_to_hole_opener_length_ft,
        {{ wv_meters_to_inches('szdriftmincalc') }} as min_drift_in,
        {{ wv_meters_to_inches('szodmaxcalc') }} as max_nominal_od_in,
        {{ wv_meters_to_inches('szodhocalc') }} as hole_opener_od_in,

        -- string weight and volumes
        {{ wv_newtons_to_klbf('weightaircalc') }} as string_weight_in_air_klbf,
        {{ wv_newtons_to_klbf('weightairnodpcalc') }} as string_weight_in_air_no_drill_pipe_klbf,
        {{ wv_cbm_to_bbl('volumedispcalc') }} as volume_displaced_bbl,
        {{ wv_cbm_to_bbl('volumeinternalcalc') }} as string_internal_volume_bbl,
        {{ wv_newtons_to_klbf('travelequipwt') }} as traveling_equipment_weight_klbf,

        -- tripping parameters
        {{ wv_newtons_to_klbf('triphookloadmax') }} as max_trip_hook_load_klbf,
        {{ wv_newtons_to_klbf('tripoverpullmax') }} as max_trip_over_pull_klbf,
        {{ wv_mps_to_ft_per_hr('triprateinavg') }} as avg_trip_rate_in_ft_per_hr,
        {{ wv_mps_to_ft_per_hr('triprateinmax') }} as max_trip_rate_in_ft_per_hr,
        {{ wv_mps_to_ft_per_hr('triprateoutavg') }} as avg_trip_rate_out_ft_per_hr,
        {{ wv_mps_to_ft_per_hr('triprateoutmax') }} as max_trip_rate_out_ft_per_hr,

        -- inclination data
        inclstringtopcalc::float as top_inclination_deg,
        inclstringbtmcalc::float as bottom_inclination_deg,
        inclstringmaxcalc::float as max_inclination_deg,

        -- equipment flags
        centralizersnotallycalc::int as number_of_centralizers_tally,
        mudmotorcalc::boolean as mud_motor_present,
        vgscalc::boolean as vgs_present,
        qtyplugcalc::int as quantity_plugs_drilled,

        -- string components and descriptions
        trim(componentscalc)::varchar as string_components,
        trim(componentsdimcalc)::varchar as string_component_dimensions,
        trim(componentsaltdimcalc)::varchar as string_component_dimensions_alt,
        trim(tripnote)::varchar as trip_note,
        trim(com)::varchar as comments,

        -- related entities
        trim(idrecwellborecalc)::varchar as wellbore_id,
        trim(idrecwellborecalctk)::varchar as wellbore_table_key,
        trim(idreclastrigcalc)::varchar as last_rig_id,
        trim(idreclastrigcalctk)::varchar as last_rig_table_key,

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
        and drill_string_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['drill_string_id']) }} as drill_string_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        drill_string_sk,

        -- identifiers
        drill_string_id,
        job_id,
        well_id,

        -- basic drill string information
        proposed_or_actual,
        drill_string_description,
        bha_number,
        proposed_version_number,

        -- bit and bha information
        drill_bit_id,
        drill_bit_table_key,
        bit_run_number,
        bit_nozzles,
        bit_nozzles_alt_format,
        iadc_bit_dull_code,
        bha_objective,
        directional_control_method,
        complexity_index,

        -- string results
        bha_result,
        bha_result_details,
        bha_result_notes,

        -- bit dull grading
        bit_dull_tooth_inner,
        bit_dull_tooth_outer,
        bit_dull_tooth_dull_char,
        bit_dull_tooth_location,
        bit_dull_bearing,
        bit_dull_gauge,
        bit_dull_other,
        bit_dull_reason_pulled,

        -- operational dates
        date_in,
        date_out,
        date_in_no_excluded_params,
        date_out_no_excluded_params,
        date_pickup,
        date_out_of_hole,
        proposed_date_in,
        proposed_date_out,

        -- bit total fluid area
        bit_total_fluid_area_sq_in,
        bit_total_fluid_area_calc_sq_in,

        -- depth performance
        proposed_depth_in_ft,
        proposed_depth_out_ft,
        depth_in_ft,
        depth_out_ft,
        depth_in_no_excluded_ft,
        depth_out_no_excluded_ft,
        depth_drilled_ft,
        depth_drilled_no_excluded_ft,
        depth_rotating_ft,
        depth_sliding_ft,

        -- cumulative bit depth
        cum_depth_on_bit_start_ft,
        cum_depth_on_bit_end_ft,
        cum_depth_on_bit_start_no_excluded_ft,
        cum_depth_on_bit_end_no_excluded_ft,

        -- time performance
        cum_time_on_bit_start_hr,
        cum_time_on_bit_end_hr,
        cum_time_on_bit_start_no_excluded_hr,
        cum_time_on_bit_end_no_excluded_hr,
        drilling_time_hr,
        drilling_time_no_excluded_hr,
        circulating_time_hr,
        tripping_time_hr,
        other_time_hr,
        circ_trip_other_time_hr,
        rotating_time_hr,
        sliding_time_hr,
        out_of_hole_to_pickup_duration_hr,
        off_bottom_duration_min,
        on_bottom_duration_min,
        pipe_moving_duration_min,

        -- rate of penetration
        avg_rop_ft_per_hr,
        avg_rop_no_excluded_ft_per_hr,
        avg_instantaneous_rop_ft_per_hr,
        rotating_rop_ft_per_hr,
        sliding_rop_ft_per_hr,

        -- drilling percentages
        percent_depth_rotating,
        percent_depth_sliding,
        percent_time_rotating,
        percent_time_sliding,

        -- rpm parameters
        bit_revolutions,
        bit_revolutions_no_excluded,
        avg_rpm,
        min_rpm,
        max_rpm,
        avg_rpm_no_excluded,
        max_rpm_no_excluded,

        -- torque parameters
        avg_drilling_torque,
        min_drilling_torque,
        max_drilling_torque,

        -- weight on bit
        avg_weight_on_bit_klbf,
        min_weight_on_bit_klbf,
        max_weight_on_bit_klbf,
        avg_weight_on_bit_no_excluded_klbf,
        min_weight_on_bit_no_excluded_klbf,
        max_weight_on_bit_no_excluded_klbf,

        -- hook load parameters
        min_rotating_hook_load_klbf,
        max_rotating_hook_load_klbf,
        min_rotating_hook_load_no_excluded_klbf,
        max_rotating_hook_load_no_excluded_klbf,
        min_off_bottom_hook_load_klbf,
        max_off_bottom_hook_load_klbf,
        min_off_bottom_hook_load_no_excluded_klbf,
        max_off_bottom_hook_load_no_excluded_klbf,
        min_pickup_hook_load_klbf,
        max_pickup_hook_load_klbf,
        min_pickup_hook_load_no_excluded_klbf,
        max_pickup_hook_load_no_excluded_klbf,
        min_slackoff_hook_load_klbf,
        max_slackoff_hook_load_klbf,
        min_slackoff_hook_load_no_excluded_klbf,
        max_slackoff_hook_load_no_excluded_klbf,

        -- flow rate parameters
        avg_flow_rate_gpm,
        min_flow_rate_gpm,
        max_flow_rate_gpm,
        avg_riser_boost_gpm,
        min_riser_boost_gpm,
        max_riser_boost_gpm,

        -- pressure parameters
        avg_standpipe_pressure_psi,
        min_standpipe_pressure_psi,
        max_standpipe_pressure_psi,
        avg_standpipe_pressure_no_excluded_psi,
        min_standpipe_pressure_no_excluded_psi,
        max_standpipe_pressure_no_excluded_psi,

        -- mud density
        avg_last_mud_density_ppg,
        min_last_mud_density_ppg,
        max_last_mud_density_ppg,
        min_mud_density_ppg,
        max_mud_density_ppg,

        -- ecd parameters
        avg_ecd_end_ppg,
        min_ecd_end_ppg,
        max_ecd_end_ppg,

        -- hydraulic power
        avg_hydraulic_power_per_area_hp_per_sq_in,
        min_hydraulic_power_per_area_hp_per_sq_in,
        max_hydraulic_power_per_area_hp_per_sq_in,

        -- string dimensions
        string_length_ft,
        bit_to_hole_opener_length_ft,
        min_drift_in,
        max_nominal_od_in,
        hole_opener_od_in,

        -- string weight and volumes
        string_weight_in_air_klbf,
        string_weight_in_air_no_drill_pipe_klbf,
        volume_displaced_bbl,
        string_internal_volume_bbl,
        traveling_equipment_weight_klbf,

        -- tripping parameters
        max_trip_hook_load_klbf,
        max_trip_over_pull_klbf,
        avg_trip_rate_in_ft_per_hr,
        max_trip_rate_in_ft_per_hr,
        avg_trip_rate_out_ft_per_hr,
        max_trip_rate_out_ft_per_hr,

        -- inclination data
        top_inclination_deg,
        bottom_inclination_deg,
        max_inclination_deg,

        -- equipment flags
        number_of_centralizers_tally,
        mud_motor_present,
        vgs_present,
        quantity_plugs_drilled,

        -- string components and descriptions
        string_components,
        string_component_dimensions,
        string_component_dimensions_alt,
        trip_note,
        comments,

        -- related entities
        wellbore_id,
        wellbore_table_key,
        last_rig_id,
        last_rig_table_key,

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
