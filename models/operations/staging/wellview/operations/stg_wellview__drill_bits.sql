{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'operations']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBDRILLBIT') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as drill_bit_id,
        trim(idrecparent)::varchar as job_id,
        trim(idwell)::varchar as well_id,
        sysseq::int as sequence_number,

        -- basic bit information
        trim(proposedoractual)::varchar as proposed_or_actual,
        trim(typ1)::varchar as bit_type,
        trim(typ2)::varchar as bit_subtype,
        trim(iconname)::varchar as icon_name,

        -- bit specifications
        {{ wv_meters_to_inches('szoddrill') }} as bit_size_in,
        {{ wv_meters_to_feet('length') }} as bit_length_ft,
        {{ wv_meters_to_feet('lengthgauge') }} as gauge_length_ft,
        {{ wv_meters_to_inches('szodpass') }} as pass_through_diameter_in,
        {{ wv_meters_to_inches('szcutter') }} as cutter_size_in,
        bladeno::int as number_of_blades,
        trim(connthrd)::varchar as connection_thread,
        {{ wv_meters_to_inches('connsz') }} as connection_size_in,

        -- bit details
        trim(make)::varchar as manufacturer,
        trim(model)::varchar as model,
        trim(sn)::varchar as serial_number,
        trim(usedclass)::varchar as condition_class,
        cost::float as bit_cost,
        trim(owner)::varchar as bit_owner,
        trim(refid)::varchar as reference_id,
        dttmmanufacture::timestamp_ntz as manufacture_date,

        -- iadc codes
        trim(iadccode1)::varchar as iadc_code_1,
        trim(iadccode2)::varchar as iadc_code_2,
        trim(iadccode3)::varchar as iadc_code_3,
        trim(iadccode4)::varchar as iadc_code_4,
        trim(iadccodescalc)::varchar as iadc_codes_combined,
        trim(bitwearoutcalc)::varchar as bit_wear_out_code,

        -- operational dates
        dttmincalc::timestamp_ntz as date_in,
        dttmoutcalc::timestamp_ntz as date_out,
        dttminnoexcludecalc::timestamp_ntz as date_in_no_excluded_params,
        dttmoutnoexcludecalc::timestamp_ntz as date_out_no_excluded_params,

        -- depth performance (converted from meters to feet)
        {{ wv_meters_to_feet('depthincalc') }} as depth_in_ft,
        {{ wv_meters_to_feet('depthoutcalc') }} as depth_out_ft,
        {{ wv_meters_to_feet('depthinnoexcludecalc') }} as depth_in_no_excluded_ft,
        {{ wv_meters_to_feet('depthoutnoexcludecalc') }} as depth_out_no_excluded_ft,
        {{ wv_meters_to_feet('depthintvdcalc') }} as depth_in_tvd_ft,
        {{ wv_meters_to_feet('depthouttvdcalc') }} as depth_out_tvd_ft,
        {{ wv_meters_to_feet('depthdrilledjobcalc') }} as depth_drilled_this_job_ft,
        {{ wv_meters_to_feet('depthdrilledtotalcalc') }} as total_depth_drilled_ft,
        {{ wv_meters_to_feet('depthdrilledstart') }} as depth_drilled_start_ft,

        -- time performance (converted from days to hours)
        {{ wv_days_to_hours('tmstart') }} as starting_hours_hr,
        {{ wv_days_to_hours('tmdrilledjobcalc') }} as drilling_time_this_job_hr,
        {{ wv_days_to_hours('tmdrilledtotalcalc') }} as total_drilling_time_hr,
        {{ wv_days_to_hours('tmcircjobcalc') }} as circulating_time_this_job_hr,
        {{ wv_days_to_hours('tmtripjobcalc') }} as tripping_time_hr,
        {{ wv_days_to_hours('tmotherjobcalc') }} as other_time_hr,
        {{ wv_days_to_hours('tmcirctripotherjobcalc') }} as circ_trip_other_time_hr,
        {{ wv_days_to_hours('tmrotatingcalc') }} as rotating_time_hr,
        {{ wv_days_to_hours('tmslidingcalc') }} as sliding_time_hr,

        -- rate of penetration (converted from m/s to ft/hr)
        {{ wv_mps_to_ft_per_hr('ropjobcalc') }} as avg_rop_this_job_ft_per_hr,
        {{ wv_mps_to_ft_per_hr('roptotalcalc') }} as total_avg_rop_ft_per_hr,
        {{ wv_mps_to_ft_per_hr('roprotatingcalc') }} as rotating_rop_ft_per_hr,
        {{ wv_mps_to_ft_per_hr('ropslidingcalc') }} as sliding_rop_ft_per_hr,

        -- drilling percentages (proportion -> percentage)
        percenttmrotatingcalc / 0.01 as percent_time_rotating,
        percenttmslidingcalc / 0.01 as percent_time_sliding,

        -- rpm parameters
        bitrevscalc::float as bit_revolutions,
        rpmavgcalc::float as avg_rpm,
        rpmmincalc::float as min_rpm,
        rpmmaxcalc::float as max_rpm,

        -- torque parameters
        torquedrillavgcalc::float as avg_drilling_torque,
        torquedrillmincalc::float as min_drilling_torque,
        torquedrillmaxcalc::float as max_drilling_torque,

        -- weight on bit (converted from newtons to klbf)
        {{ wv_newtons_to_klbf('wobavgcalc') }} as avg_weight_on_bit_klbf,
        {{ wv_newtons_to_klbf('wobmincalc') }} as min_weight_on_bit_klbf,
        {{ wv_newtons_to_klbf('wobmaxcalc') }} as max_weight_on_bit_klbf,

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

        -- mud properties (converted from kg/m3 to lb/gal)
        {{ wv_kgm3_to_lb_per_gal('densitylastmudchkavgcalc') }} as avg_mud_density_ppg,
        {{ wv_kgm3_to_lb_per_gal('densitylastmudchkmincalc') }} as min_mud_density_ppg,
        {{ wv_kgm3_to_lb_per_gal('densitylastmudchkmaxcalc') }} as max_mud_density_ppg,

        -- ecd parameters (converted from kg/m3 to lb/gal)
        {{ wv_kgm3_to_lb_per_gal('ecdendavgcalc') }} as avg_ecd_end_ppg,
        {{ wv_kgm3_to_lb_per_gal('ecdendmincalc') }} as min_ecd_end_ppg,
        {{ wv_kgm3_to_lb_per_gal('ecdendmaxcalc') }} as max_ecd_end_ppg,

        -- hydraulic power (watts/m2 -> hp/in2: / 1155837.15667431)
        hydpwrperareaavgcalc / 1155837.15667431 as avg_hydraulic_power_per_area_hp_per_sq_in,
        hydpwrperareamincalc / 1155837.15667431 as min_hydraulic_power_per_area_hp_per_sq_in,
        hydpwrperareamaxcalc / 1155837.15667431 as max_hydraulic_power_per_area_hp_per_sq_in,

        -- other performance metrics
        qtyplugcalc::int as quantity_plugs_drilled,
        trim(bitconsid)::varchar as bit_considerations,
        trim(note)::varchar as notes,

        -- related entities
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
        and drill_bit_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['drill_bit_id']) }} as drill_bit_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        drill_bit_sk,

        -- identifiers
        drill_bit_id,
        job_id,
        well_id,
        sequence_number,

        -- basic bit information
        proposed_or_actual,
        bit_type,
        bit_subtype,
        icon_name,

        -- bit specifications
        bit_size_in,
        bit_length_ft,
        gauge_length_ft,
        pass_through_diameter_in,
        cutter_size_in,
        number_of_blades,
        connection_thread,
        connection_size_in,

        -- bit details
        manufacturer,
        model,
        serial_number,
        condition_class,
        bit_cost,
        bit_owner,
        reference_id,
        manufacture_date,

        -- iadc codes
        iadc_code_1,
        iadc_code_2,
        iadc_code_3,
        iadc_code_4,
        iadc_codes_combined,
        bit_wear_out_code,

        -- operational dates
        date_in,
        date_out,
        date_in_no_excluded_params,
        date_out_no_excluded_params,

        -- depth performance
        depth_in_ft,
        depth_out_ft,
        depth_in_no_excluded_ft,
        depth_out_no_excluded_ft,
        depth_in_tvd_ft,
        depth_out_tvd_ft,
        depth_drilled_this_job_ft,
        total_depth_drilled_ft,
        depth_drilled_start_ft,

        -- time performance
        starting_hours_hr,
        drilling_time_this_job_hr,
        total_drilling_time_hr,
        circulating_time_this_job_hr,
        tripping_time_hr,
        other_time_hr,
        circ_trip_other_time_hr,
        rotating_time_hr,
        sliding_time_hr,

        -- rate of penetration
        avg_rop_this_job_ft_per_hr,
        total_avg_rop_ft_per_hr,
        rotating_rop_ft_per_hr,
        sliding_rop_ft_per_hr,

        -- drilling percentages
        percent_time_rotating,
        percent_time_sliding,

        -- rpm parameters
        bit_revolutions,
        avg_rpm,
        min_rpm,
        max_rpm,

        -- torque parameters
        avg_drilling_torque,
        min_drilling_torque,
        max_drilling_torque,

        -- weight on bit
        avg_weight_on_bit_klbf,
        min_weight_on_bit_klbf,
        max_weight_on_bit_klbf,

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

        -- mud properties
        avg_mud_density_ppg,
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

        -- other performance metrics
        quantity_plugs_drilled,
        bit_considerations,
        notes,

        -- related entities
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
