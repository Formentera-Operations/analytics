{{
    config(
        materialized='view',
        tags=['procount', 'staging', 'crescent']
    )
}}

with

source as (

    select * from {{ source('procount', 'DOWNTIMEREASONTB') }}

),

renamed as (

    select
        -- identifiers
        objectmerrickid::number as object_merrick_id,
        objecttype::number as object_type,
        trim(riodowntimeid)::varchar as riodowntime_id,
        commentserviceid::number as commentservice_id,
        downtimecode::number as downtime_code,

        -- dates
        originaldateentered::timestamp_ntz as originaldateentered,
        trim(originaltimeentered)::varchar as originaltimeentered,
        startdate::timestamp_ntz as start_date,
        trim(starttime)::varchar as starttime,
        startproductiondate::timestamp_ntz as startproduction_date,
        userdatestamp::timestamp_ntz as user_date_stamp,
        datetimestamp::timestamp_ntz as date_timestamp,
        endproductiondate::timestamp_ntz as endproduction_date,
        calcdowntimeflag::number as calcdowntime_flag,
        allocationdatestamp::timestamp_ntz as allocation_date_stamp,
        enddate::timestamp_ntz as end_date,
        trim(endtime)::varchar as endtime,
        dateentryflag::number as dateentry_flag,
        lastloaddate::timestamp_ntz as lastload_date,
        trim(lastloadtime)::varchar as lastloadtime,
        downtimehours::float as downtime_hours,
        blogicdatestamp::timestamp_ntz as blogic_date_stamp,

        -- names and descriptions
        trim(comments)::varchar as comments,

        -- volumes
        lostproduction::float as lostproduction,

        -- operational/equipment
        lastdayhoursdown::float as lastdayhoursdown,

        -- flags
        messagesendflag::number as messagesend_flag,
        transmitflag::number as transmit_flag,
        updownflag::number as updown_flag,
        deleteflag::number as delete_flag,

        -- audit/metadata
        userid::number as user_id,
        trim(rowuid)::varchar as rowu_id,
        trim(usertimestamp)::varchar as user_timestamp,
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_tz as _fivetran_synced,

        -- other
        repaircosts::float as repaircosts,
        lasttransmission::number as lasttransmission,
        destinationperson::number as destinationperson,
        trim(blogictimestamp)::varchar as blogic_timestamp,
        trim(reason)::varchar as reason

    from source

),

filtered as (

    select *
    from renamed
    where coalesce(_fivetran_deleted, false) = false
      and object_merrick_id is not null

),

enhanced as (

    select
        {{ dbt_utils.generate_surrogate_key(['object_merrick_id', 'object_type', 'originaldateentered', 'originaltimeentered']) }} as downtimereason_sk,
        *,
        current_timestamp() as _loaded_at

    from filtered

),

final as (

    select
        downtimereason_sk,

        -- identifiers
        object_merrick_id,
        object_type,
        riodowntime_id,
        commentservice_id,
        downtime_code,

        -- dates
        originaldateentered,
        originaltimeentered,
        start_date,
        starttime,
        startproduction_date,
        user_date_stamp,
        date_timestamp,
        endproduction_date,
        calcdowntime_flag,
        allocation_date_stamp,
        end_date,
        endtime,
        dateentry_flag,
        lastload_date,
        lastloadtime,
        downtime_hours,
        blogic_date_stamp,

        -- names and descriptions
        comments,

        -- volumes
        lostproduction,

        -- operational/equipment
        lastdayhoursdown,

        -- flags
        messagesend_flag,
        transmit_flag,
        updown_flag,
        delete_flag,

        -- audit/metadata
        user_id,
        rowu_id,
        user_timestamp,
        _fivetran_deleted,
        _fivetran_synced,

        -- other
        repaircosts,
        lasttransmission,
        destinationperson,
        blogic_timestamp,
        reason,

        -- dbt metadata
        _loaded_at

    from enhanced

)

select * from final