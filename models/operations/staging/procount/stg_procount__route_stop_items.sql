{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for IFS Procount route stop items.
    
    ONE-TO-ONE with source table: route_stop_item_tb_sheet_1
    
    No joins - just rename and light casting.
*/

with source as (
    select * from {{ source('seeds_raw', 'route_stop_item_tb_sheet_1') }}
),

renamed as (
    select
        -- Primary Key
        _line as procount_line_number,
        
        -- Foreign Keys
        routestopmerrickid as route_stop_id,
        objectmerrickid as object_merrick_id,
        objectmerricktype as object_merrick_type,
        
        -- Configuration
        stoptype as stop_type,
        itemorder as item_order,
        
        -- Display Flags
        displayoil as display_oil,
        displaygas as display_gas,
        displaywater as display_water,
        
        -- Flags
        transmitflag as transmit_flag,
        backgroundtaskflag as background_task_flag,
        
        -- Dates
        lastloaddate as last_load_date,
        
        -- Metadata
        userid as user_id,
        userdatestamp as user_date_stamp,
        usertimestamp as user_time_stamp,
        datetimestamp as date_time_stamp,
        lasttransmission as last_transmission,
        lastloadtime as last_load_time,
        
        -- Fivetran
        _fivetran_synced

    from source
)

select * from renamed