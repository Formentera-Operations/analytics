{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for IFS Procount tank strapping headers and details.
    
    NOTE: Combining two related tables:
    - tank_strapping_tb_barnett_sheet_1 (headers)
    - tank_strapping_detail_tb_barnett_sheet_1 (details)
    
    For staging, we'll create two separate models to maintain 1:1 principle.
*/

-- Header Model
with source as (
    select * from {{ source('seeds_raw', 'tank_strapping_tb_barnett_sheet_1') }}
),

renamed as (
    select
        -- Primary Key
        _line as procount_line_number,
        row_uid,
        
        -- Foreign Key
        tank_id,
        
        -- Strapping Info
        description,
        strapping_data_type,
        total_tank_capacity,
        effective_date,
        record_date,
        
        -- Business Entity
        purchaser_beid as purchaser_be_id,
        
        -- Dates
        last_load_date,
        
        -- Flags
        transmit_flag,
        background_task_flag,
        
        -- Metadata
        user_id,
        user_date_stamp,
        user_time_stamp,
        date_time_stamp,
        blogic_date_stamp,
        blogic_time_stamp,
        last_transmission,
        last_load_time,
        
        -- Fivetran
        _fivetran_synced

    from source
)

select * from renamed