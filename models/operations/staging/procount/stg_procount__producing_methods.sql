{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for IFS Procount producing methods reference.
    
    ONE-TO-ONE with source table: producing_methods_tb_sheet_1
    
    No joins - just rename and light casting.
*/

with source as (
    select * from {{ source('seeds_raw', 'producing_methods_tb_sheet_1') }}
),

renamed as (
    select
        -- Primary Key
        _line as procount_line_number,
        producingmethodsmerrickid as producing_methods_id,
        
        -- Description
        producingmethodsdescription as producing_methods_description,
        
        -- IDs
        productionid as production_id,
        accountingid as accounting_id,
        engineeringid as engineering_id,
        
        -- Codes
        riomethodcode as rio_method_code,
        louisianawelltestcode as louisiana_well_test_code,
        entryscreentype as entry_screen_type,
        
        -- State Codes
        arkansasstatecode as arkansas_state_code,
        californiastatecode as california_state_code,
        coloradostatecode as colorado_state_code,
        michiganstatecode as michigan_state_code,
        mississippistatecode as mississippi_state_code,
        newmexicostatecode as new_mexico_state_code,
        northdakotastatecode as north_dakota_state_code,
        southdakotastatecode as south_dakota_state_code,
        texasstatecode as texas_state_code,
        utahstatecode as utah_state_code,
        
        -- Flags
        activeflag as active_flag,
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