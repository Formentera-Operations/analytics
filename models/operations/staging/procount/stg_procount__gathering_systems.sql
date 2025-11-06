{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for IFS Procount gathering systems.
    
    ONE-TO-ONE with source table: gathering_systemtb_barnett_sheet_1
    
    No joins - just rename and light casting.
*/

with source as (
    select * from {{ source('seeds_raw', 'gathering_systemtb_barnett_sheet_1') }}
),

renamed as (
    select
        -- Primary Key
        _line as procount_line_number,
        gatheringsystemmerrickid as gathering_system_id,
        
        -- Business Identifiers
        gatheringsystemname as gathering_system_name,
        gatheringsystemnameshort as gathering_system_name_short,
        
        -- Configuration
        producttype as product_type,
        allocationprocess as allocation_process,
        walkupflag as walkup_flag,
        lockeditingwithgslock as locked_editing_with_gs_lock,
        
        -- Owner
        ownerid as owner_id,
        ownerloc as owner_loc,
        
        -- Foreign Keys
        batteryid as battery_id,
        platformid as platform_id,
        
        -- Geographic
        areaid as area_id,
        divisionid as division_id,
        fieldgroupid as field_group_id,
        groupid as group_id,
        
        -- Personnel
        pumperpersonid as pumper_person_id,
        foremanpersonid as foreman_person_id,
        superpersonid as super_person_id,
        accountantpersonid as accountant_person_id,
        engineerpersonid as engineer_person_id,
        
        -- Teams
        productionteamid as production_team_id,
        drillingteamid as drilling_team_id,
        accountingteamid as accounting_team_id,
        
        -- Dates
        startactivedate as start_active_date,
        endactivedate as end_active_date,
        lastallocationdate as last_allocation_date,
        dataeditstartdate as data_edit_start_date,
        
        -- Flags
        activeflag as active_flag,
        printflag as print_flag,
        allocautoflag as alloc_auto_flag,
        accountinguploadflag as accounting_upload_flag,
        engineeringuploadflag as engineering_upload_flag,
        
        -- Metadata
        userid as user_id,
        userdatestamp as user_date_stamp,
        usertimestamp as user_time_stamp,
        datetimestamp as date_time_stamp,
        rowuid as row_uid

    from source
)

select * from renamed