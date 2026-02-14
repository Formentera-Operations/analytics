{{ config(
    materialized='view',
    tags=['wellview', 'drilling', 'drillstring', 'components', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBDRILLSTRINGCOMP') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as drill_string_component_id,
        idrecparent as drill_string_id,
        idwell as well_id,
        sysseq as sequence_number,

        -- Basic component information
        des as item_description,
        iconname as icon_name,
        compsubtyp as equipment_type,
        grade as grade,
        joints as number_of_joints,
        jointstallycalc as joints_in_tally,
        itemnocalc as item_number,
        desjtcalc as description_with_joints,

        -- Physical dimensions (converted to US units)
        conntyptop as top_connection_type,
        connthrdtop as top_connection_thread,
        upsettop as top_upset,
        conntypbtm as bottom_connection_type,
        connthrdbtm as bottom_connection_thread,
        upsetbtm as bottom_upset,
        connectcalc as connections,

        -- Fishing neck dimensions (converted to US units)
        connectaltcalc as connections_alt_format,
        make as manufacturer,

        -- Weight specifications (converted to US units)
        model as model,
        sn as serial_number,
        material as material,

        -- Volume calculations (converted to US units)
        coating as coating,
        service as service_type,

        -- Connection specifications
        owner as owner,
        refid as reference_id,
        usedclass as condition_class,
        conditionrun as condition_run,
        conditionpull as condition_pull,
        currentstatus as current_status,
        currentstatuscalc as current_status_calculated,
        dttmlastinspect as last_inspection_date,
        dayssinceinspectcalc as days_since_last_inspection,
        dttmmanufacture as manufacture_date,

        -- Torque specifications (converted to US units)
        dttmstatuscalc as current_status_date,
        comptotalruncalc as total_number_of_runs,

        -- Ratings and specifications (converted to US units)
        radioactivesource as radioactive_source,
        linetosurf as line_to_surface,

        -- Component details
        centralizersnotallycalc as number_of_centralizers_tally,
        cost as item_cost,
        costunitlabel as cost_unit_label,
        com as comments,
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,

        -- Condition and status
        systag as system_tag,
        syslockdate as system_lock_date,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockmeui as system_lock_me_ui,

        -- Operational history (converted to US units)
        syslockchildrenui as system_lock_children_ui,
        _fivetran_synced as fivetran_synced_at,
        szodnom / 0.0254 as nominal_od_in,
        szidnom / 0.0254 as nominal_id_in,
        szodmax / 0.0254 as max_od_in,
        szdrift / 0.0254 as drift_in,

        -- Job performance metrics (converted to US units)
        length / 0.3048 as length_ft,
        lengthcumcalc / 0.3048 as cumulative_length_ft,
        lengthtallycalc / 0.3048 as tally_length_ft,
        fishneckod / 0.0254 as fishing_neck_od_in,

        -- Special equipment features
        fishnecklength / 0.3048 as fishing_neck_length_ft,
        wtperlength / 1.48816394356955 as weight_per_length_lb_per_ft,
        weightcalc / 4.4482216152605 as component_weight_lbf,

        -- Cost information
        weightcumcalc / 4448.2216152605 as cumulative_weight_klbf,
        volumeinternalcalc / 0.158987294928 as internal_volume_bbl,

        -- Comments
        volumedispcumcalc / 0.158987294928 as cumulative_volume_displaced_bbl,

        -- System fields
        connsztop / 0.0254 as top_connection_size_in,
        connszbtm / 0.0254 as bottom_connection_size_in,
        torquemin / 1.3558179483314 as makeup_torque_ft_lb,
        torquemax / 1.3558179483314 as max_torque_ft_lb,
        tensilemax / 4448.2216152605 as max_tensile_strength_klbf,
        temprating / 0.555555555555556 + 32 as temperature_rating_deg_f,
        hoursstart / 0.0416666666666667 as starting_hours_hr,
        hoursendcalc / 0.0416666666666667 as ending_hours_hr,
        depthdrilledjobcalc / 0.3048 as depth_drilled_this_job_ft,
        tmdrilledjobcalc / 0.0416666666666667 as drilling_time_this_job_hr,

        -- Fivetran fields
        tmcircjobcalc / 0.0416666666666667 as circulating_time_this_job_hr

    from source_data
)

select * from renamed
