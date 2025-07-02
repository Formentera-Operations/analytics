{{ config(
    materialized='view',
    tags=['wellview', 'drilling', 'bits', 'performance', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBDRILLBIT') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as drill_bit_id,
        idrecparent as job_id,
        idwell as well_id,
        sysseq as sequence_number,
        
        -- Basic bit information
        proposedoractual as proposed_or_actual,
        typ1 as bit_type,
        typ2 as bit_subtype,
        iconname as icon_name,
        
        -- Bit specifications (converted to US units)
        szoddrill / 0.0254 as bit_size_in,
        length / 0.3048 as bit_length_ft,
        lengthgauge / 0.3048 as gauge_length_ft,
        szodpass / 0.0254 as pass_through_diameter_in,
        szcutter / 0.0254 as cutter_size_in,
        bladeno as number_of_blades,
        
        -- Connection details (converted to US units)
        connsz / 0.0254 as connection_size_in,
        connthrd as connection_thread,
        
        -- Bit details
        make as manufacturer,
        model as model,
        sn as serial_number,
        usedclass as condition_class,
        cost as bit_cost,
        owner as bit_owner,
        refid as reference_id,
        dttmmanufacture as manufacture_date,
        
        -- IADC codes
        iadccode1 as iadc_code_1,
        iadccode2 as iadc_code_2,
        iadccode3 as iadc_code_3,
        iadccode4 as iadc_code_4,
        iadccodescalc as iadc_codes_combined,
        bitwearoutcalc as bit_wear_out_code,
        
        -- Operational dates
        dttmincalc as date_in,
        dttmoutcalc as date_out,
        dttminnoexcludecalc as date_in_no_excluded_params,
        dttmoutnoexcludecalc as date_out_no_excluded_params,
        
        -- Depth performance (converted to US units)
        depthincalc / 0.3048 as depth_in_ft,
        depthoutcalc / 0.3048 as depth_out_ft,
        depthinnoexcludecalc / 0.3048 as depth_in_no_excluded_ft,
        depthoutnoexcludecalc / 0.3048 as depth_out_no_excluded_ft,
        depthintvdcalc / 0.3048 as depth_in_tvd_ft,
        depthouttvdcalc / 0.3048 as depth_out_tvd_ft,
        
        -- Drilling performance (converted to US units)
        depthdrilledjobcalc / 0.3048 as depth_drilled_this_job_ft,
        depthdrilledtotalcalc / 0.3048 as total_depth_drilled_ft,
        depthdrilledstart / 0.3048 as depth_drilled_start_ft,
        
        -- Time performance (converted to US units)
        tmstart / 0.0416666666666667 as starting_hours_hr,
        tmdrilledjobcalc / 0.0416666666666667 as drilling_time_this_job_hr,
        tmdrilledtotalcalc / 0.0416666666666667 as total_drilling_time_hr,
        tmcircjobcalc / 0.0416666666666667 as circulating_time_this_job_hr,
        tmtripjobcalc / 0.0416666666666667 as tripping_time_hr,
        tmotherjobcalc / 0.0416666666666667 as other_time_hr,
        tmcirctripotherjobcalc / 0.0416666666666667 as circ_trip_other_time_hr,
        tmrotatingcalc / 0.0416666666666667 as rotating_time_hr,
        tmslidingcalc / 0.0416666666666667 as sliding_time_hr,
        
        -- Rate of penetration (converted to US units)
        ropjobcalc / 7.3152 as avg_rop_this_job_ft_per_hr,
        roptotalcalc / 7.3152 as total_avg_rop_ft_per_hr,
        roprotatingcalc / 7.3152 as rotating_rop_ft_per_hr,
        ropslidingcalc / 7.3152 as sliding_rop_ft_per_hr,
        
        -- Drilling parameters
        bitrevscalc as bit_revolutions,
        percenttmrotatingcalc / 0.01 as percent_time_rotating,
        percenttmslidingcalc / 0.01 as percent_time_sliding,
        
        -- Weight on bit (converted to US units)
        wobavgcalc / 4448.2216152605 as avg_weight_on_bit_klbf,
        wobmincalc / 4448.2216152605 as min_weight_on_bit_klbf,
        wobmaxcalc / 4448.2216152605 as max_weight_on_bit_klbf,
        
        -- RPM parameters
        rpmavgcalc as avg_rpm,
        rpmmincalc as min_rpm,
        rpmmaxcalc as max_rpm,
        
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
        
        -- Torque parameters (no conversion needed - units not specified in view)
        torquedrillavgcalc as avg_drilling_torque,
        torquedrillmincalc as min_drilling_torque,
        torquedrillmaxcalc as max_drilling_torque,
        
        -- Mud properties (converted to US units)
        densitylastmudchkavgcalc / 119.826428404623 as avg_mud_density_ppg,
        densitylastmudchkmincalc / 119.826428404623 as min_mud_density_ppg,
        densitylastmudchkmaxcalc / 119.826428404623 as max_mud_density_ppg,
        
        -- ECD parameters (converted to US units)
        ecdendavgcalc / 119.826428404623 as avg_ecd_end_ppg,
        ecdendmincalc / 119.826428404623 as min_ecd_end_ppg,
        ecdendmaxcalc / 119.826428404623 as max_ecd_end_ppg,
        
        -- Hydraulic power (converted to US units)
        hydpwrperareaavgcalc / 1155837.15667431 as avg_hydraulic_power_per_area_hp_per_sq_in,
        hydpwrperareamincalc / 1155837.15667431 as min_hydraulic_power_per_area_hp_per_sq_in,
        hydpwrperareamaxcalc / 1155837.15667431 as max_hydraulic_power_per_area_hp_per_sq_in,
        
        -- Other performance metrics
        qtyplugcalc as quantity_plugs_drilled,
        bitconsid as bit_considerations,
        note as notes,
        
        -- Related entities
        idreclastrigcalc as last_rig_id,
        idreclastrigcalctk as last_rig_table_key,
        
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