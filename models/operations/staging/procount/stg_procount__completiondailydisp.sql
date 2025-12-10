{{
    config(
        materialized='view',
        tags=['procount', 'staging', 'crescent']
    )
}}

with

source as (

    select * from {{ source('procount', 'COMPLETIONDAILYDISPTB') }}

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
        datetimestamp::timestamp_ntz as date_timestamp,
        userdatestamp::timestamp_ntz as user_date_stamp,
        allocationdatestamp::timestamp_ntz as allocation_date_stamp,
        runticketdate::timestamp_ntz as run_ticket_date,

        -- volumes
        allocestwatervol::float as alloc_est_water_vol,
        allocestinjoilvol::float as alloc_est_inj_oil_vol,
        allocestinjgasvolmcf::float as alloc_est_inj_gas_vol_mcf,
        allocestinjwatervol::float as alloc_est_inj_water_vol,
        allocestco2vol::float as alloc_est_co2_vol,
        allocestothervol::float as alloc_est_other_vol,
        allocestnglvol::float as alloc_est_ngl_vol,
        allocestinjgasvolmmbtu::float as alloc_est_inj_gas_vol_mmbtu,
        allocestgasvolmcf::float as alloc_est_gas_vol_mcf,
        allocestinjothervol::float as alloc_est_inj_other_vol,
        allocestgasvolmmbtu::float as alloc_est_gas_vol_mmbtu,
        allocestinjco2vol::float as alloc_est_inj_co2_vol,
        allocestoilvol::float as alloc_est_oil_vol,

        -- pressures
        allocestpressurebase::float as alloc_est_pressure_base,

        -- temperatures
        allocesttemperature::float as alloc_est_temperature,

        -- allocation factors
        allocestbtufactor::float as alloc_est_btu_factor,
        allocestgravity::float as alloc_est_gravity,

        -- flags
        allocestwetdryflag::number as alloc_est_wet_dry_flag,
        trim(processedflag)::varchar as processed_flag,
        runticketflag::number as run_ticket_flag,
        overwriteflag::number as overwrite_flag,

        -- audit/metadata
        trim(usertimestamp)::varchar as user_timestamp,
        userid::number as user_id,
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_tz as _fivetran_synced,

        -- other
        productcode::number as product_code,
        allocestmass::float as alloc_est_mass,
        producttype::number as product_type,
        sequencenumber::number as sequence_number,
        allocestplantgasmmbtu::float as alloc_est_plant_gas_mmbtu,
        allocestplantgasmcf::float as alloc_est_plant_gas_mcf,
        dispositioncode::number as disposition_code,
        allocestsulfurmass::float as alloc_est_sulfur_mass,
        trim(allocationtimestamp)::varchar as allocation_timestamp

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
        {{ dbt_utils.generate_surrogate_key(['merrick_id', 'record_date', 'run_ticket_number', 'source_id', 'source_type']) }} as completiondailydisp_sk,
        *,
        current_timestamp() as _loaded_at

    from filtered

),

final as (

    select
        completiondailydisp_sk,

        -- identifiers
        merrick_id,
        run_ticket_number,
        source_id,
        source_type,
        gathering_system_id,

        -- dates
        record_date,
        date_timestamp,
        user_date_stamp,
        allocation_date_stamp,
        run_ticket_date,

        -- volumes
        alloc_est_water_vol,
        alloc_est_inj_oil_vol,
        alloc_est_inj_gas_vol_mcf,
        alloc_est_inj_water_vol,
        alloc_est_co2_vol,
        alloc_est_other_vol,
        alloc_est_ngl_vol,
        alloc_est_inj_gas_vol_mmbtu,
        alloc_est_gas_vol_mcf,
        alloc_est_inj_other_vol,
        alloc_est_gas_vol_mmbtu,
        alloc_est_inj_co2_vol,
        alloc_est_oil_vol,

        -- pressures
        alloc_est_pressure_base,

        -- temperatures
        alloc_est_temperature,

        -- allocation factors
        alloc_est_btu_factor,
        alloc_est_gravity,

        -- flags
        alloc_est_wet_dry_flag,
        processed_flag,
        run_ticket_flag,
        overwrite_flag,

        -- audit/metadata
        user_timestamp,
        user_id,
        _fivetran_deleted,
        _fivetran_synced,

        -- other
        product_code,
        alloc_est_mass,
        product_type,
        sequence_number,
        alloc_est_plant_gas_mmbtu,
        alloc_est_plant_gas_mcf,
        disposition_code,
        alloc_est_sulfur_mass,
        allocation_timestamp,

        -- dbt metadata
        _loaded_at

    from enhanced

)

select * from final