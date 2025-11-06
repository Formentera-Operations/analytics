{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for IFS Procount connections.
    
    ONE-TO-ONE with source table: connect_tb_sheet_1
    
    IMPORTANT: This table has Excel serial dates that need conversion.
*/

with source as (
    select * from {{ source('seeds_raw', 'connect_tb_sheet_1') }}
),

renamed as (
    select
        -- Primary Key
        _line as procount_line_number,
        rowuid as row_uid,
        
        -- Upstream Object (polymorphic)
        upstreamid as upstream_id,
        upstreamtype as upstream_type,
        
        -- Downstream Object (polymorphic)
        downstreamid as downstream_id,
        downstreamtype as downstream_type,
        
        -- Description
        description,
        
        -- Dates (already TIMESTAMP_TZ in source - no conversion needed)
        startdate as start_date,
        enddate as end_date,
        startmonth as start_month,
        endmonth as end_month,
        
        -- Flags
        deleteflag as delete_flag,
        backgroundtaskflag as background_task_flag,
        
        -- Metadata
        userid as user_id,
        userdatestamp as user_date_stamp,
        usertimestamp as user_time_stamp,
        datetimestamp as date_time_stamp,
        
        -- Fivetran
        _fivetran_synced,
        _fivetran_deleted

    from source
)

select * from renamed