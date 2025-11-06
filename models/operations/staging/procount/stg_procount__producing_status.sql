{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for IFS Procount producing status reference.
    
    ONE-TO-ONE with source table: producing_status_tb_sheet_1
    
    No joins - just rename and light casting.
*/

with source as (
    select * from {{ source('seeds_raw', 'producing_status_tb_sheet_1') }}
),

renamed as (
    select
        -- Primary Key
        _line as procount_line_number,
        producingstatusmerrickid as producing_status_id,
        
        -- Descriptions
        producingdescription as producing_description,
        producingdescriptionshort as producing_description_short,
        
        -- Codes
        accountingid as accounting_id,
        riostatuscode as rio_status_code,
        mmsogorcode as mms_ogor_code,
        
        -- State Codes
        alabamastatecode as alabama_state_code,
        alaskastatecode as alaska_state_code,
        arkansasstatecode as arkansas_state_code,
        californiastatecode as california_state_code,
        coloradostatecode as colorado_state_code,
        louisianastatecode as louisiana_state_code,
        michiganstatecode as michigan_state_code,
        mississippistatecode as mississippi_state_code,
        montanastatecode as montana_state_code,
        northdakotastatecode as north_dakota_state_code,
        southdakotastatecode as south_dakota_state_code,
        texasstatecode as texas_state_code,
        utahstatecode as utah_state_code,
        
        -- Flags
        activeflag as active_flag,
        wellactiveindicator as well_active_indicator,
        hoursontotal_24_flag as hours_on_total_24_flag,
        
        -- Metadata
        userid as user_id,
        userdatestamp as user_date_stamp,
        usertimestamp as user_time_stamp,
        datetimestamp as date_time_stamp,
        
        -- Fivetran
        _fivetran_synced

    from source
)

select * from renamed