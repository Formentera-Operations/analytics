{{
    config(
        materialized='view',
        tags=['procount', 'staging', 'crescent']
    )
}}

with

source as (

    select * from {{ source('procount', 'DOWNTIMECODETB') }}

),

renamed as (

    select
        -- identifiers
        downtimecode::number as downtime_code,
        trim(accountingid)::varchar as accounting_id,
        trim(engineeringid)::varchar as engineering_id,
        parentcode::number as parent_code,
        trim(productionid)::varchar as production_id,

        -- dates
        trim(downtimedescriptionlevel4)::varchar as downtimedescriptionlevel4,
        datetimestamp::timestamp_ntz as date_timestamp,
        trim(downtimedescriptionlevel1)::varchar as downtimedescriptionlevel1,
        trim(downtimedescription)::varchar as downtimedescription,
        trim(downtimedescriptionlevel3)::varchar as downtimedescriptionlevel3,
        userdatestamp::timestamp_ntz as user_date_stamp,
        trim(riodowntimecode)::varchar as riodowntime_code,
        trim(downtimedescriptionlevel2)::varchar as downtimedescriptionlevel2,

        -- well/completion attributes
        producingmethod::number as producingmethod,
        defaultproducingstatus::number as defaultproducingstatus,
        producingstatus::number as producingstatus,

        -- geography
        trim(californiareasoncode)::varchar as californiareason_code,

        -- temperatures
        tempinteger::number as tempinteger,

        -- flags
        activeflag::number as active_flag,
        commentrequiredflag::number as commentrequired_flag,
        updownflag::number as updown_flag,

        -- audit/metadata
        trim(usertimestamp)::varchar as user_timestamp,
        userid::number as user_id,
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_tz as _fivetran_synced,

        -- other
        trim(mmsreasoncode)::varchar as mmsreason_code,
        objectmerricktype::number as objectmerrick_type,
        alternatecode::number as alternate_code

    from source

),

filtered as (

    select *
    from renamed
    where coalesce(_fivetran_deleted, false) = false
      and downtime_code is not null

),

enhanced as (

    select
        *,
        current_timestamp() as _loaded_at

    from filtered

),

final as (

    select
        -- identifiers
        downtime_code,
        accounting_id,
        engineering_id,
        parent_code,
        production_id,

        -- dates
        downtimedescriptionlevel4,
        date_timestamp,
        downtimedescriptionlevel1,
        downtimedescription,
        downtimedescriptionlevel3,
        user_date_stamp,
        riodowntime_code,
        downtimedescriptionlevel2,

        -- well/completion attributes
        producingmethod,
        defaultproducingstatus,
        producingstatus,

        -- geography
        californiareason_code,

        -- temperatures
        tempinteger,

        -- flags
        active_flag,
        commentrequired_flag,
        updown_flag,

        -- audit/metadata
        user_timestamp,
        user_id,
        _fivetran_deleted,
        _fivetran_synced,

        -- other
        mmsreason_code,
        objectmerrick_type,
        alternate_code,

        -- dbt metadata
        _loaded_at

    from enhanced

)

select * from final