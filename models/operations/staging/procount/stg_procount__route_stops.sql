{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for IFS Procount route stops.
    
    ONE-TO-ONE with source table: route_stop_tb_sheet_1
    
    No joins - just rename and light casting.
*/

with source as (
    select * from {{ source('seeds_raw', 'route_stop_tb_sheet_1') }}
),

renamed as (
    select
        -- Primary Key
        _line as procount_line_number,
        routestopmerrickid as route_stop_id,
        
        -- Foreign Keys
        routemerrickid as route_id,
        locationmerrickid as location_merrick_id,
        
        -- Business Identifier
        stopname as stop_name,
        stoptype as stop_type,
        stoporder as stop_order,
        productionid as production_id,
        
        -- Flags
        activeflag as active_flag,
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