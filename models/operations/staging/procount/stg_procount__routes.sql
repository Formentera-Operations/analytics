{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for IFS Procount routes.
    
    ONE-TO-ONE with source table: route_tb_barnett_sheet_1
    
    No joins - just rename and light casting.
*/

with source as (
    select * from {{ source('seeds_raw', 'route_tb_barnett_sheet_1') }}
),

renamed as (
    select
        -- Primary Key
        _line as procount_line_number,
        routemerrickid as route_id,
        
        -- Business Identifier
        routename as route_name,
        
        -- Source Meters
        sourcemeteroil as source_meter_oil,
        sourcemetergas as source_meter_gas,
        sourcemeterwater as source_meter_water,
        sourcemeteroilinj as source_meter_oil_inj,
        sourcemetergasinj as source_meter_gas_inj,
        sourcemeterwaterinj as source_meter_water_inj,
        
        -- Personnel (multiple slots)
        pumperid as pumper_id,
        pumperid_2 as pumper_id_2,
        pumperid_3 as pumper_id_3,
        pumperid_4 as pumper_id_4,
        pumperid_5 as pumper_id_5,
        pumperid_6 as pumper_id_6,
        
        foremanid as foreman_id,
        foremanid_2 as foreman_id_2,
        foremanid_3 as foreman_id_3,
        foremanid_4 as foreman_id_4,
        
        accountantid as accountant_id,
        accountantid_2 as accountant_id_2,
        accountantid_3 as accountant_id_3,
        accountantid_4 as accountant_id_4,
        
        -- Teams
        productionteamid as production_team_id,
        accountingteamid as accounting_team_id,
        divisionid as division_id,
        
        -- Dates
        startactivedate as start_active_date,
        endactivedate as end_active_date,
        latestcompleteddate as latest_completed_date,
        latestuploadeddate as latest_uploaded_date,
        latestmessagedate as latest_message_date,
        lastloaddate as last_load_date,
        
        -- Flags
        activeflag as active_flag,
        printflag as print_flag,
        allocateonreplication as allocate_on_replication,
        transmitflag as transmit_flag,
        backgroundtaskflag as background_task_flag,
        
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