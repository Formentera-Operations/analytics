{{
    config(
        materialized='view',
        tags=['procount', 'staging', 'crescent']
    )
}}

with

source as (

    select * from {{ source('procount', 'TANKDAILYTB') }}

),

renamed as (

    select
        -- identifiers
        merrickid::number as merrick_id,
        commentid::number as comment_id,

        -- dates
        recorddate::timestamp_ntz as record_date,
        userdatestamp::timestamp_ntz as user_date_stamp,
        allocationdatestamp::timestamp_ntz as allocation_date_stamp,
        datetimestamp::timestamp_ntz as date_timestamp,
        trim(lastloadtime)::varchar as lastloadtime,
        blogicdatestamp::timestamp_ntz as blogic_date_stamp,
        strappingdate::timestamp_ntz as strapping_date,
        lastloaddate::timestamp_ntz as lastload_date,
        productiondate::timestamp_ntz as production_date,
        trim(tankgaugetime)::varchar as tankgaugetime,

        -- volumes
        adjustedproductionwater::float as adjustedproductionwater,
        grossbarrelsoil::float as grossbarrelsoil,
        productionngl::float as productionngl,
        adjustedproductionoil::float as adjustedproductionoil,
        productionallocdefault::number as productionallocdefault,
        productionwater::float as productionwater,
        productionoil::float as productionoil,
        othervolume::float as othervolume,

        -- rates
        rateoil::float as rateoil,
        ratewater::float as ratewater,

        -- pressures
        gaugepressure::float as gaugepressure,

        -- temperatures
        temperaturefactor::float as temperaturefactor,
        gaugetemperature::float as gaugetemperature,
        ambienttemperature::float as ambienttemperature,
        observedtemperature::float as observedtemperature,

        -- allocation factors
        tankvaporfactor::float as tankvaporfactor,
        gasequivalentfactor::float as gasequivalentfactor,
        observedgravity::float as observedgravity,
        factorcomputationflag::number as factorcomputation_flag,
        convertedgravity::float as convertedgravity,

        -- operational/equipment
        gaugehours::float as gaugehours,

        -- flags
        loadtransferflag::number as loadtransfer_flag,
        transmitflag::number as transmit_flag,
        firstdayflag::number as firstday_flag,
        tankdataentryflag::number as tankdataentry_flag,
        calculationstatusflag::number as calculationstatus_flag,
        backgroundtaskflag::number as backgroundtask_flag,
        tankisolatedflag::number as tankisolated_flag,

        -- audit/metadata
        trim(usertimestamp)::varchar as user_timestamp,
        trim(rowuid)::varchar as rowu_id,
        userid::number as user_id,
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_tz as _fivetran_synced,

        -- other
        percentfullngl::float as percentfullngl,
        bottomquarter::float as bottomquarter,
        dispositionallocdefault::number as dispositionallocdefault,
        dispositioncode::number as disposition_code,
        tankvaporvolume::float as tankvaporvolume,
        waterinventoryadjustment::float as waterinventoryadjustment,
        endingoil::float as endingoil,
        bottomtotalinches::float as bottomtotalinches,
        beginningwater::float as beginningwater,
        adjustedbeginningwater::float as adjustedbeginningwater,
        adjustedendingwater::float as adjustedendingwater,
        bottomfeet::number as bottomfeet,
        totalrunsngl::float as totalrunsngl,
        totalrunswater::float as totalrunswater,
        errornumber::number as error_number,
        productcode::number as product_code,
        oilinventoryadjustment::float as oilinventoryadjustment,
        adjustedendingoil::float as adjustedendingoil,
        tankcompbeginningoil::float as tankcompbeginningoil,
        topinch::number as topinch,
        grossngl::float as grossngl,
        allocationorder::number as allocationorder,
        allocationmethod::number as allocationmethod,
        grosstotalrunsoil::float as grosstotalrunsoil,
        endingwater::float as endingwater,
        adjustedbeginningoil::float as adjustedbeginningoil,
        totalrunsoil::float as totalrunsoil,
        trim(allocationtimestamp)::varchar as allocation_timestamp,
        lasttransmission::number as lasttransmission,
        watercut::float as watercut,
        producttype::number as product_type,
        bsandw::float as bsandw,
        beginningngl::float as beginningngl,
        topfeet::number as topfeet,
        allocationbycomponent::number as allocationbycomponent,
        bottominch::number as bottominch,
        tankcompbeginningwater::float as tankcompbeginningwater,
        trim(blogictimestamp)::varchar as blogic_timestamp,
        datasourcecode::number as datasource_code,
        beginningoil::float as beginningoil,
        toptotalinches::float as toptotalinches,
        endingngl::float as endingngl,
        topquarter::float as topquarter

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
        {{ dbt_utils.generate_surrogate_key(['merrick_id', 'record_date']) }} as tankdaily_sk,
        *,
        current_timestamp() as _loaded_at

    from filtered

),

final as (

    select
        tankdaily_sk,

        -- identifiers
        merrick_id,
        comment_id,

        -- dates
        record_date,
        user_date_stamp,
        allocation_date_stamp,
        date_timestamp,
        lastloadtime,
        blogic_date_stamp,
        strapping_date,
        lastload_date,
        production_date,
        tankgaugetime,

        -- volumes
        adjustedproductionwater,
        grossbarrelsoil,
        productionngl,
        adjustedproductionoil,
        productionallocdefault,
        productionwater,
        productionoil,
        othervolume,

        -- rates
        rateoil,
        ratewater,

        -- pressures
        gaugepressure,

        -- temperatures
        temperaturefactor,
        gaugetemperature,
        ambienttemperature,
        observedtemperature,

        -- allocation factors
        tankvaporfactor,
        gasequivalentfactor,
        observedgravity,
        factorcomputation_flag,
        convertedgravity,

        -- operational/equipment
        gaugehours,

        -- flags
        loadtransfer_flag,
        transmit_flag,
        firstday_flag,
        tankdataentry_flag,
        calculationstatus_flag,
        backgroundtask_flag,
        tankisolated_flag,

        -- audit/metadata
        user_timestamp,
        rowu_id,
        user_id,
        _fivetran_deleted,
        _fivetran_synced,

        -- other
        percentfullngl,
        bottomquarter,
        dispositionallocdefault,
        disposition_code,
        tankvaporvolume,
        waterinventoryadjustment,
        endingoil,
        bottomtotalinches,
        beginningwater,
        adjustedbeginningwater,
        adjustedendingwater,
        bottomfeet,
        totalrunsngl,
        totalrunswater,
        error_number,
        product_code,
        oilinventoryadjustment,
        adjustedendingoil,
        tankcompbeginningoil,
        topinch,
        grossngl,
        allocationorder,
        allocationmethod,
        grosstotalrunsoil,
        endingwater,
        adjustedbeginningoil,
        totalrunsoil,
        allocation_timestamp,
        lasttransmission,
        watercut,
        product_type,
        bsandw,
        beginningngl,
        topfeet,
        allocationbycomponent,
        bottominch,
        tankcompbeginningwater,
        blogic_timestamp,
        datasource_code,
        beginningoil,
        toptotalinches,
        endingngl,
        topquarter,

        -- dbt metadata
        _loaded_at

    from enhanced

)

select * from final