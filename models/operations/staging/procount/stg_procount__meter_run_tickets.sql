{{
    config(
        materialized='view',
        tags=['procount', 'staging', 'crescent']
    )
}}

with

source as (

    select * from {{ source('procount', 'METERRUNTICKETTB') }}

),

renamed as (

    select
        -- identifiers
        merrickid::number as merrick_id,
        trim(runticketnumber)::varchar as run_ticket_number,
        haulerstatementid::number as haulerstatement_id,
        trim(purchasermeterid)::varchar as purchasermeter_id,
        trim(runno)::varchar as runno,
        destinationobjectid::number as destinationobject_id,
        reidvaporpressure::float as reidvaporpressure,
        fluiddisposition::number as fluiddisposition,
        trim(haulermeterid)::varchar as haulermeter_id,
        sourceid::number as source_id,
        purchaserstatementid::number as purchaserstatement_id,
        sourcetype::number as source_type,
        originationobjectid::number as originationobject_id,
        commentserviceid::number as commentservice_id,
        trim(regulatoryfacilityid)::varchar as regulatoryfacility_id,

        -- dates
        recorddate::timestamp_ntz as record_date,
        dateinservice::timestamp_ntz as dateinservice,
        sampledate::timestamp_ntz as sample_date,
        trim(runclosetime)::varchar as runclosetime,
        haulerstatementdate::timestamp_ntz as haulerstatement_date,
        runopendate::timestamp_ntz as runopen_date,
        lastloaddate::timestamp_ntz as lastload_date,
        trim(runopentime)::varchar as runopentime,
        runticketdate::timestamp_ntz as run_ticket_date,
        allocationdatestamp::timestamp_ntz as allocation_date_stamp,
        userdatestamp::timestamp_ntz as user_date_stamp,
        runclosedate::timestamp_ntz as runclose_date,
        dateinstalled::timestamp_ntz as dateinstalled,
        dateproved::timestamp_ntz as dateproved,
        trim(lastloadtime)::varchar as lastloadtime,
        blogicdatestamp::timestamp_ntz as blogic_date_stamp,

        -- well/completion attributes
        apiversion::number as apiversion,

        -- names and descriptions
        trim(comments)::varchar as comments,

        -- geography
        haulerstatementseq::number as haulerstatementseq,
        purchaserstatementseq::number as purchaserstatementseq,

        -- accounting/business entities
        purchaser::number as purchaser,
        purchaserloc::number as purchaserloc,
        haulerreportedbarrels::float as haulerreportedbarrels,
        haulerloc::number as haulerloc,
        hauler::number as hauler,

        -- volumes
        grossbarrels::float as grossbarrels,
        netbarrels::float as netbarrels,
        haulerreportednetbarrels::float as haulerreportednetbarrels,

        -- pressures
        oilmetergaugepressure::float as oilmetergaugepressure,
        linepressure::float as linepressure,
        trim(pressurefactor)::varchar as pressurefactor,
        psifactor::number as psifactor,

        -- temperatures
        linetemperature::float as linetemperature,
        closetemperature::float as closetemperature,
        opentemperature::float as opentemperature,
        observedtemperature::float as observedtemperature,
        temperaturecompensation::number as temperaturecompensation,

        -- allocation factors
        meterfactor::number as meterfactor,
        convertedgravity::float as convertedgravity,
        allocatedgravity::float as allocatedgravity,
        lactcompressibilityfactor::float as lactcompressibilityfactor,
        haulerreportedconvertedgravity::float as haulerreportedconvertedgravity,
        lactmeterfactor::float as lactmeterfactor,
        actualgravity::float as actualgravity,

        -- flags
        runordispositionflag::number as runordisposition_flag,
        transmitflag::number as transmit_flag,
        usehaulerbarrelsflag::number as usehaulerbarrels_flag,
        actualestimatedflag::number as actualestimated_flag,
        deleteflag::number as delete_flag,
        unitsflag::number as units_flag,
        calculationstatusflag::number as calculationstatus_flag,
        backgroundtaskflag::number as backgroundtask_flag,

        -- audit/metadata
        trim(usertimestamp)::varchar as user_timestamp,
        trim(rowuid)::varchar as rowu_id,
        userid::number as user_id,
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_tz as _fivetran_synced,

        -- other
        allocatedngl::float as allocatedngl,
        netngl::float as netngl,
        allocatedwaterbarrels::float as allocatedwaterbarrels,
        watercarryover::float as watercarryover,
        adjustednet::float as adjustednet,
        originationobjecttype::number as originationobject_type,
        trim(allocationtimestamp)::varchar as allocation_timestamp,
        metermalfunction::float as metermalfunction,
        grossngl::float as grossngl,
        producttype::number as product_type,
        adjustedgross::float as adjustedgross,
        dispositioncode::number as disposition_code,
        lasttransmission::number as lasttransmission,
        trim(destination)::varchar as destination,
        trim(blogictimestamp)::varchar as blogic_timestamp,
        datasourcecode::number as datasource_code,
        allocatedoilbarrels::float as allocatedoilbarrels,
        netwaterbarrels::float as netwaterbarrels,
        productcode::number as product_code,
        destinationobjecttype::number as destinationobject_type,
        watervaporcontentppm::float as watervaporcontentppm,
        emulsionpercent::float as emulsionpercent,
        saltcontentppm::float as saltcontentppm,
        gasblowby::float as gasblowby,
        bsandw::float as bsandw,
        lactdensitycorrection::float as lactdensitycorrection,
        sulfurcontentppm::float as sulfurcontentppm,
        openodometer::float as openodometer,
        waxpercent::float as waxpercent,
        closeodometer::float as closeodometer

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
        {{ dbt_utils.generate_surrogate_key(['merrick_id', 'record_date', 'run_ticket_number']) }} as meterrunticket_sk,
        *,
        current_timestamp() as _loaded_at

    from filtered

),

final as (

    select
        meterrunticket_sk,

        -- identifiers
        merrick_id,
        run_ticket_number,
        haulerstatement_id,
        purchasermeter_id,
        runno,
        destinationobject_id,
        reidvaporpressure,
        fluiddisposition,
        haulermeter_id,
        source_id,
        purchaserstatement_id,
        source_type,
        originationobject_id,
        commentservice_id,
        regulatoryfacility_id,

        -- dates
        record_date,
        dateinservice,
        sample_date,
        runclosetime,
        haulerstatement_date,
        runopen_date,
        lastload_date,
        runopentime,
        run_ticket_date,
        allocation_date_stamp,
        user_date_stamp,
        runclose_date,
        dateinstalled,
        dateproved,
        lastloadtime,
        blogic_date_stamp,

        -- well/completion attributes
        apiversion,

        -- names and descriptions
        comments,

        -- geography
        haulerstatementseq,
        purchaserstatementseq,

        -- accounting/business entities
        purchaser,
        purchaserloc,
        haulerreportedbarrels,
        haulerloc,
        hauler,

        -- volumes
        grossbarrels,
        netbarrels,
        haulerreportednetbarrels,

        -- pressures
        oilmetergaugepressure,
        linepressure,
        pressurefactor,
        psifactor,

        -- temperatures
        linetemperature,
        closetemperature,
        opentemperature,
        observedtemperature,
        temperaturecompensation,

        -- allocation factors
        meterfactor,
        convertedgravity,
        allocatedgravity,
        lactcompressibilityfactor,
        haulerreportedconvertedgravity,
        lactmeterfactor,
        actualgravity,

        -- flags
        runordisposition_flag,
        transmit_flag,
        usehaulerbarrels_flag,
        actualestimated_flag,
        delete_flag,
        units_flag,
        calculationstatus_flag,
        backgroundtask_flag,

        -- audit/metadata
        user_timestamp,
        rowu_id,
        user_id,
        _fivetran_deleted,
        _fivetran_synced,

        -- other
        allocatedngl,
        netngl,
        allocatedwaterbarrels,
        watercarryover,
        adjustednet,
        originationobject_type,
        allocation_timestamp,
        metermalfunction,
        grossngl,
        product_type,
        adjustedgross,
        disposition_code,
        lasttransmission,
        destination,
        blogic_timestamp,
        datasource_code,
        allocatedoilbarrels,
        netwaterbarrels,
        product_code,
        destinationobject_type,
        watervaporcontentppm,
        emulsionpercent,
        saltcontentppm,
        gasblowby,
        bsandw,
        lactdensitycorrection,
        sulfurcontentppm,
        openodometer,
        waxpercent,
        closeodometer,

        -- dbt metadata
        _loaded_at

    from enhanced

)

select * from final