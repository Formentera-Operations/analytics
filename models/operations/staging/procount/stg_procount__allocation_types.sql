{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for IFS Procount allocation types.
    
    NOTE: There are 4 allocation type tables:
    - completion_allocation_type_tb
    - equipment_allocation_type_tb
    - meter_allocation_type_tb
    - tank_allocation_type_tb
    
    For staging, create separate models for each to maintain 1:1 principle.
    This is the COMPLETION allocation types model.
*/

with source as (
    select * from {{ source('seeds_raw', 'completion_allocation_type_tb_sheet_1') }}
),

renamed as (
    select
        -- Primary Key
        _line as procount_line_number,
        allocation_type,
        row_uid,
        
        -- Names
        allocation_name,
        description,
        
        -- Target
        target_type,
        
        -- Daily (D_) Flags & Settings
        d_data_population_flag,
        d_entered_as_allocated_flag,
        d_allocation_active_flag,
        d_factor_computation_flag,
        
        -- Monthly (M_) Flags & Settings
        m_data_population_flag,
        m_entered_as_allocated_flag,
        m_allocation_active_flag,
        m_factor_computation_flag,
        
        -- System (S_) Flags
        s_outside_operated_flag,
        s_carry_forward_flag,
        s_units_type_flag,
        s_tests_enabled_flag,
        
        -- Daily Allocation Factors (1-8)
        d_allocation_factor,
        d_allocation_factor_2,
        d_allocation_factor_3,
        d_allocation_factor_4,
        d_allocation_factor_5,
        d_allocation_factor_6,
        d_allocation_factor_7,
        d_allocation_factor_8,
        
        -- Daily Allocation Factor Types
        d_allocation_factor_type,
        d_allocation_factor_type_2,
        d_allocation_factor_type_3,
        d_allocation_factor_type_4,
        d_allocation_factor_type_5,
        d_allocation_factor_type_6,
        d_allocation_factor_type_7,
        d_allocation_factor_type_8,
        
        -- Daily Equipment Allocation Factors (1-4)
        d_equip_alloc_factor,
        d_equip_alloc_factor_2,
        d_equip_alloc_factor_3,
        d_equip_alloc_factor_4,
        
        -- Daily Equipment Allocation Factor Types
        d_equip_alloc_factor_type,
        d_equip_alloc_factor_type_2,
        d_equip_alloc_factor_type_3,
        d_equip_alloc_factor_type_4,
        
        -- Monthly Allocation Factors (1-8)
        m_allocation_factor,
        m_allocation_factor_2,
        m_allocation_factor_3,
        m_allocation_factor_4,
        m_allocation_factor_5,
        m_allocation_factor_6,
        m_allocation_factor_7,
        m_allocation_factor_8,
        
        -- Monthly Allocation Factor Types
        m_allocation_factor_type,
        m_allocation_factor_type_2,
        m_allocation_factor_type_3,
        m_allocation_factor_type_4,
        m_allocation_factor_type_5,
        m_allocation_factor_type_6,
        m_allocation_factor_type_7,
        m_allocation_factor_type_8,
        
        -- Monthly Equipment Allocation Factors (1-4)
        m_equip_alloc_factor,
        m_equip_alloc_factor_2,
        m_equip_alloc_factor_3,
        m_equip_alloc_factor_4,
        
        -- Monthly Equipment Allocation Factor Types
        m_equip_alloc_factor_type,
        m_equip_alloc_factor_type_2,
        m_equip_alloc_factor_type_3,
        m_equip_alloc_factor_type_4,
        
        -- Flag
        active_flag,
        
        -- Metadata
        user_id,
        user_date_stamp,
        user_time_stamp,
        
        -- Fivetran
        _fivetran_synced

    from source
)

select * from renamed