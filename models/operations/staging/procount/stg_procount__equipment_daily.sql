{{
    config(
        materialized='view',
        tags=['procount', 'staging', 'crescent']
    )
}}

with

source as (

    select * from {{ source('procount', 'EQUIPMENTDAILYTB') }}

),

renamed as (

    select
        -- identifiers
        merrickid::number as merrick_id,
        commentid::number as comment_id,
        downtimecode::number as downtime_code,

        -- dates
        recorddate::timestamp_ntz as record_date,
        lastentereddatedays::number as lastentereddatedays,
        trim(lastloadtime)::varchar as lastloadtime,
        lastloaddate::timestamp_ntz as lastload_date,
        allocationdatestamp::timestamp_ntz as allocation_date_stamp,
        productiondate::timestamp_ntz as production_date,
        datetimestamp::timestamp_ntz as date_timestamp,
        lastentereddate::timestamp_ntz as lastentered_date,
        downtimehours::float as downtime_hours,
        blogicdatestamp::timestamp_ntz as blogic_date_stamp,
        userdatestamp::timestamp_ntz as user_date_stamp,

        -- volumes
        actoilvol::float as actoilvol,
        convothervol::float as convothervol,
        convgasvolmmbtu::float as convgas_vol_mmbtu,
        convoilvol::float as convoilvol,
        actothervol::float as actothervol,
        convwatervol::float as convwatervol,
        convgasvolmcf::float as convgas_vol_mcf,
        actgasvolmmbtu::float as actgas_vol_mmbtu,
        actwatervol::float as actwatervol,
        actgasvolmcf::float as actgas_vol_mcf,

        -- rates
        ratecomputationflag::number as ratecomputation_flag,
        ratewater::float as ratewater,
        rateoil::float as rateoil,
        rategas::float as rategas,

        -- pressures
        suctionpressure::float as suctionpressure,
        engineoilpressure::float as engineoilpressure,
        pumpingpressure::float as pumpingpressure,
        stage4pressure::float as stage4pressure,
        stage3pressure::float as stage3pressure,
        stage2pressure::float as stage2pressure,
        intakepressure::float as intakepressure,
        stage1pressure::float as stage1pressure,
        actpressurebase::float as actpressurebase,
        convpressurebase::float as convpressurebase,
        oilpressure::number as oilpressure,
        compressoroilpressure::float as compressoroilpressure,
        dischargepressure::float as dischargepressure,

        -- temperatures
        acttemperature::float as acttemperature,
        exhaust1temp::float as exhaust1temp,
        suctiontemp::float as suctiontemp,
        stage2temp::float as stage2temp,
        stage1temp::float as stage1temp,
        enginewatertemp::float as enginewatertemp,
        outletoiltemp::float as outletoiltemp,
        compressoroiltemp::float as compressoroiltemp,
        inletoiltemp::float as inletoiltemp,
        engineoiltemp::float as engineoiltemp,
        convtemperature::float as convtemperature,
        compressorwatertemp::float as compressorwatertemp,
        exchangetemp::float as exchangetemp,
        stage4temp::float as stage4temp,
        exhaust2temp::float as exhaust2temp,
        stage3temp::float as stage3temp,
        dischargetemp::float as dischargetemp,
        enginetemperature::number as enginetemperature,

        -- allocation factors
        actgravity::float as actgravity,
        convbtufactor::float as convbtufactor,
        leaseusecoefficient::float as leaseusecoefficient,
        convgravity::float as convgravity,
        actheatfactor::float as actheatfactor,
        leaseusecoefficienttype::number as leaseusecoefficient_type,
        actbtufactor::float as actbtufactor,
        convheatfactor::float as convheatfactor,

        -- operational/equipment
        strokesperminute::float as strokesperminute,
        suctionorboost2::float as suctionorboost2,
        hourson::float as hourson,
        suctionorboost1::float as suctionorboost1,
        hoursopen::float as hoursopen,
        hoursclose::float as hoursclose,
        enginerpm::float as enginerpm,

        -- flags
        backgroundtaskflag::number as backgroundtask_flag,
        actwetdryflag::number as actwetdry_flag,
        volumeautopopulateflag::number as volumeautopopulate_flag,
        transmitflag::number as transmit_flag,
        calculationstatusflag::number as calculationstatus_flag,
        stilldownflag::number as stilldown_flag,
        convwetdryflag::number as convwetdry_flag,
        hoursonautopopulateflag::number as hoursonautopopulate_flag,

        -- audit/metadata
        usernumber5::float as usernumber5,
        trim(userstringa)::varchar as userstringa,
        trim(rowuid)::varchar as rowu_id,
        usernumber1::float as usernumber1,
        usernumber4::float as usernumber4,
        userid::number as user_id,
        trim(usertimestamp)::varchar as user_timestamp,
        usernumber3::float as usernumber3,
        usernumber6::float as usernumber6,
        trim(userstringb)::varchar as userstringb,
        usernumber2::float as usernumber2,
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_tz as _fivetran_synced,

        -- other
        taxable::number as taxable,
        allocationbycomponent::number as allocationbycomponent,
        trim(blogictimestamp)::varchar as blogic_timestamp,
        allocationorder::number as allocationorder,
        lasttransmission::number as lasttransmission,
        dispositioncode::number as disposition_code,
        allocationmethod::number as allocationmethod,
        trim(allocationtimestamp)::varchar as allocation_timestamp,
        errornumber::number as error_number,
        producttype::number as product_type,
        lubricationoilused::float as lubricationoilused,
        datasourcecode::number as datasource_code,
        productcode::number as product_code

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
        {{ dbt_utils.generate_surrogate_key(['merrick_id', 'record_date']) }} as equipmentdaily_sk,
        *,
        current_timestamp() as _loaded_at

    from filtered

),

final as (

    select
        equipmentdaily_sk,

        -- identifiers
        merrick_id,
        comment_id,
        downtime_code,

        -- dates
        record_date,
        lastentereddatedays,
        lastloadtime,
        lastload_date,
        allocation_date_stamp,
        production_date,
        date_timestamp,
        lastentered_date,
        downtime_hours,
        blogic_date_stamp,
        user_date_stamp,

        -- volumes
        actoilvol,
        convothervol,
        convgas_vol_mmbtu,
        convoilvol,
        actothervol,
        convwatervol,
        convgas_vol_mcf,
        actgas_vol_mmbtu,
        actwatervol,
        actgas_vol_mcf,

        -- rates
        ratecomputation_flag,
        ratewater,
        rateoil,
        rategas,

        -- pressures
        suctionpressure,
        engineoilpressure,
        pumpingpressure,
        stage4pressure,
        stage3pressure,
        stage2pressure,
        intakepressure,
        stage1pressure,
        actpressurebase,
        convpressurebase,
        oilpressure,
        compressoroilpressure,
        dischargepressure,

        -- temperatures
        acttemperature,
        exhaust1temp,
        suctiontemp,
        stage2temp,
        stage1temp,
        enginewatertemp,
        outletoiltemp,
        compressoroiltemp,
        inletoiltemp,
        engineoiltemp,
        convtemperature,
        compressorwatertemp,
        exchangetemp,
        stage4temp,
        exhaust2temp,
        stage3temp,
        dischargetemp,
        enginetemperature,

        -- allocation factors
        actgravity,
        convbtufactor,
        leaseusecoefficient,
        convgravity,
        actheatfactor,
        leaseusecoefficient_type,
        actbtufactor,
        convheatfactor,

        -- operational/equipment
        strokesperminute,
        suctionorboost2,
        hourson,
        suctionorboost1,
        hoursopen,
        hoursclose,
        enginerpm,

        -- flags
        backgroundtask_flag,
        actwetdry_flag,
        volumeautopopulate_flag,
        transmit_flag,
        calculationstatus_flag,
        stilldown_flag,
        convwetdry_flag,
        hoursonautopopulate_flag,

        -- audit/metadata
        usernumber5,
        userstringa,
        rowu_id,
        usernumber1,
        usernumber4,
        user_id,
        user_timestamp,
        usernumber3,
        usernumber6,
        userstringb,
        usernumber2,
        _fivetran_deleted,
        _fivetran_synced,

        -- other
        taxable,
        allocationbycomponent,
        blogic_timestamp,
        allocationorder,
        lasttransmission,
        disposition_code,
        allocationmethod,
        allocation_timestamp,
        error_number,
        product_type,
        lubricationoilused,
        datasource_code,
        product_code,

        -- dbt metadata
        _loaded_at

    from enhanced

)

select * from final