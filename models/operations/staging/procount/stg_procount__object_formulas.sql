{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for IFS Procount object formulas.
    
    ONE-TO-ONE with source table: object_formula_tb_barnett_sheet_1
    
    No joins - just rename and light casting.
*/

with source as (
    select * from {{ source('seeds_raw', 'object_formula_tb_barnett_sheet_1') }}
),

renamed as (
    select
        -- Primary Key
        _line as procount_line_number,
        
        -- Object Reference
        object_id,
        object_type,
        
        -- Formula Details
        object_formula_type,
        formula,
        formula_description,
        formula_priority,
        
        -- Dates
        start_date,
        end_date,
        
        -- Flags
        active_flag,
        delete_flag,
        
        -- Metadata
        user_id,
        user_date_stamp,
        user_time_stamp,
        
        -- Fivetran
        _fivetran_synced

    from source
)

select * from renamed