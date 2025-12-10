{{
    config(
        materialized='view',
        tags=['procount', 'staging', 'crescent']
    )
}}

with

source as (

    select * from {{ source('procount', 'COMPLETIONMONTHLYDISPTB') }}

),

renamed as (

    select
        -- identifiers
        merrickid::number as merrick_id,
        trim(runticketnumber)::varchar as run_ticket_number,
        sourceid::number as source_id,
        sourcetype::number as source_type,
        gatheringsystemid::float as gathering_system_id,

        -- dates
        recorddate::timestamp_ntz as record_date,
        userdatestamp::timestamp_ntz as user_date_stamp,
        allocationdatestamp::timestamp_ntz as allocation_date_stamp,
        datetimestamp::timestamp_ntz as date_timestamp,
        runticketdate::timestamp_ntz as run_ticket_date,

        -- volumes
        allocactinjwatervol::float as allocactinjwatervol,
        allocactco2vol::float as allocactco2vol,
        allocactgasvolmcf::float as allocactgas_vol_mcf,
        allocactgasvolmmbtu::float as allocactgas_vol_mmbtu,
        allocactinjgasvolmcf::float as allocactinjgas_vol_mcf,
        allocactnglvol::float as allocactnglvol,
        allocactwatervol::float as allocactwatervol,
        allocactothervol::float as allocactothervol,
        allocactinjgasvolmmbtu::float as allocactinjgas_vol_mmbtu,
        allocactinjothervol::float as allocactinjothervol,
        allocactoilvol::float as allocactoilvol,
        allocactinjco2vol::float as allocactinjco2vol,
        allocactinjoilvol::float as allocactinjoilvol,

        -- pressures
        allocactpressurebase::float as allocactpressurebase,

        -- temperatures
        allocacttemperature::float as allocacttemperature,

        -- allocation factors
        allocactbtufactor::float as allocactbtufactor,
        allocactgravity::float as allocactgravity,

        -- flags
        allocactwetdryflag::number as allocactwetdry_flag,
        overwriteflag::number as overwrite_flag,
        runticketflag::number as run_ticket_flag,

        -- audit/metadata
        trim(usertimestamp)::varchar as user_timestamp,
        userid::number as user_id,
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_tz as _fivetran_synced,

        -- other
        producttype::number as product_type,
        dispositioncode::number as disposition_code,
        allocactplantgasmmbtu::float as allocactplantgasmmbtu,
        sequencenumber::number as sequence_number,
        allocactplantgasmcf::float as allocactplantgasmcf,
        allocactsulfurmass::float as allocactsulfurmass,
        trim(allocationtimestamp)::varchar as allocation_timestamp,
        productcode::number as product_code,
        allocactmass::float as allocactmass

    from source

),

filtered as (

    select *
    from renamed
    where coalesce(_fivetran_deleted, false) = false
      and merrick_id is not null

),

enhanced as (

    select
        {{ dbt_utils.generate_surrogate_key(['merrick_id', 'record_date', 'run_ticket_number', 'source_id', 'source_type']) }} as completionmonthlydisp_sk,
        *,
        current_timestamp() as _loaded_at

    from filtered

),

final as (

    select
        completionmonthlydisp_sk,

        -- identifiers
        merrick_id,
        run_ticket_number,
        source_id,
        source_type,
        gathering_system_id,

        -- dates
        record_date,
        user_date_stamp,
        allocation_date_stamp,
        date_timestamp,
        run_ticket_date,

        -- volumes
        allocactinjwatervol,
        allocactco2vol,
        allocactgas_vol_mcf,
        allocactgas_vol_mmbtu,
        allocactinjgas_vol_mcf,
        allocactnglvol,
        allocactwatervol,
        allocactothervol,
        allocactinjgas_vol_mmbtu,
        allocactinjothervol,
        allocactoilvol,
        allocactinjco2vol,
        allocactinjoilvol,

        -- pressures
        allocactpressurebase,

        -- temperatures
        allocacttemperature,

        -- allocation factors
        allocactbtufactor,
        allocactgravity,

        -- flags
        allocactwetdry_flag,
        overwrite_flag,
        run_ticket_flag,

        -- audit/metadata
        user_timestamp,
        user_id,
        _fivetran_deleted,
        _fivetran_synced,

        -- other
        product_type,
        disposition_code,
        allocactplantgasmmbtu,
        sequence_number,
        allocactplantgasmcf,
        allocactsulfurmass,
        allocation_timestamp,
        product_code,
        allocactmass,

        -- dbt metadata
        _loaded_at

    from enhanced

)

select * from final