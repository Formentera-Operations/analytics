{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for IFS Procount cross reference.
    
    ONE-TO-ONE with source table: cross_reference_tb_barnett_sheet_1
    
    No joins - just rename and light casting.
*/

with source as (
    select * from {{ source('seeds_raw', 'cross_reference_tb_barnett_sheet_1') }}
),

renamed as (
    select
        -- Primary Key
        _line as procount_line_number,
        merrick_id,
        
        -- Object Info
        object_type,
        object_name,
        object_number,
        merrick_type,
        
        -- Geographic
        state_id,
        county_id,
        area_id,
        division_id,
        field_group_id,
        group_id,
        lease_id,
        platform_id,
        
        -- Facilities
        battery_id,
        plant_id,
        ddaid,
        
        -- Teams
        production_team_id,
        drilling_team_id,
        accounting_team_id,
        
        -- Flags
        active_flag,
        print_flag,
        
        -- Fivetran
        _fivetran_synced

    from source
)

select * from renamed