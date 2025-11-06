{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for IFS Procount property/well information.
    
    ONE-TO-ONE with source table: property_well_info_tb_barnett_sheet_1
    
    This is a simple rename/recast layer - no business logic, no filtering.
*/

with source as (
    select * from {{ source('seeds_raw', 'property_well_info_tb_barnett_sheet_1') }}
),

renamed as (
    select
        -- Primary Keys
        _line as procount_line_number,
        property_well_merrick_id as well_id,
        
        -- Identifiers
        api_well_number,
        uwi,
        uwidisplay as uwi_display,
        well_name,
        
        -- Well Classification
        well_fluid_type,
        well_type,
        well_status,
        well_outcome,
        well_purpose,
        well_flow_direction,
        producing_status,
        producing_method,
        
        -- Depths (already DECIMAL in source)
        total_depth,
        target_depth,
        true_vertical_depth,
        plug_back_total_depth,
        plug_back_vertical_depth,
        elevation,
        kelly_bushing_level,
        
        -- Coordinates (already FLOAT in source)
        latitude,
        longitude,
        latitude_surface,
        longitude_surface,
        latitude_bottom_hole,
        longitude_bottom_hole,
        map_coordinate_x,
        map_coordinate_y,
        
        -- Dates (already DATE in source - no casting needed)
        spud_date,
        date_completed,
        rig_release_date,
        date_total_depth_reached,
        date_of_plug_abandon,
        date_well_acquired,
        date_abandoned,
        first_oil_sales,
        first_gas_sales,
        joint_oper_agreement_date,
        reserve_setting_book_year,
        spccplan_date,
        
        -- Timestamps (already TIMESTAMP_TZ in source - no casting needed)
        date_of_initial_production_oil,
        date_of_initial_production_gas,
        date_well_sold,
        start_active_date,
        end_active_date,
        
        -- Foreign Keys
        operator_entity_id,
        operator_entity_loc,
        county_id,
        state_id,
        country_id,
        area_id,
        division_id,
        lease_id,
        group_id,
        platform_id,
        battery_id,
        field_group_id,
        gathering_system_id,
        unit_id,
        prospect_id,
        original_unit_well_merrick_id,
        
        -- Personnel
        pumper_person_id,
        foreman_person_id,
        super_person_id,
        accountant_person_id,
        engineer_person_id,
        
        -- Teams
        production_team_id,
        drilling_team_id,
        accounting_team_id,
        
        -- Financial
        acreage,
        sales_code,
        acquisition_code,
        before_casing_point_bill_deck,
        after_casing_point_bill_deck,
        
        -- Flags (keep as NUMBER, convert to boolean in intermediate layer)
        active_flag,
        delete_flag,
        outside_operated_flag,
        offshore_flag,
        template_record_flag,
        template_record_used,
        print_flag,
        accounting_upload_flag,
        engineering_upload_flag,
        supply_tracking_flag,
        
        -- Configuration
        units_type_flag,
        uwitype as uwi_type,
        no_of_completion as completion_count,
        completion_child_count,
        flash_month_offset,
        data_source_code,
        data_base_version_number_ext_bld,
        ddaid,
        
        -- User Fields
        user_number_1,
        user_number_2,
        user_number_3,
        user_number_4,
        user_number_5,
        user_number_6,
        user_number_1_label,
        user_number_2_label,
        user_number_3_label,
        user_number_4_label,
        user_number_5_label,
        user_number_6_label,
        user_string_alabel,
        user_string_blabel,
        user_string_clabel,
        user_string_dlabel,
        user_string_elabel,
        user_string_flabel,
        
        -- Metadata (already correct types - no casting needed)
        user_id,
        user_date_stamp,
        user_time_stamp,
        date_time_stamp,
        blogic_date_stamp,
        blogic_time_stamp,
        row_uid

    from source
)

select * from renamed