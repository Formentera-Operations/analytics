{{
    config(
        materialized='view',
        tags=['procount', 'staging', 'crescent']
    )
}}

with

source as (

    select * from {{ source('procount', 'TANKRUNTICKETTB') }}

),

renamed as (

    select
        -- identifiers
        merrickid::number as merrick_id,
        trim(runticketnumber)::varchar as run_ticket_number,
        sourceid::number as source_id,
        purchaserstatementid::number as purchaserstatement_id,
        originationobjectid::number as originationobject_id,
        trim(haulertankid)::varchar as haulertank_id,
        reidvaporpressure::float as reidvaporpressure,
        sourcetype::number as source_type,
        destinationobjectid::number as destinationobject_id,
        destinationlocationid::number as destinationlocation_id,
        trim(purchasertankid)::varchar as purchasertank_id,
        trim(regulatoryfacilityid)::varchar as regulatoryfacility_id,
        haulerstatementid::number as haulerstatement_id,
        fluiddisposition::number as fluiddisposition,
        commentid::number as comment_id,
        sourcelocationid::number as sourcelocation_id,

        -- dates
        recorddate::timestamp_ntz as record_date,
        trim(runclosetime)::varchar as runclosetime,
        trim(lastloadtime)::varchar as lastloadtime,
        datetimestamp::timestamp_ntz as date_timestamp,
        trim(runopentime)::varchar as runopentime,
        runclosedate::timestamp_ntz as runclose_date,
        userdatestamp::timestamp_ntz as user_date_stamp,
        lastloaddate::timestamp_ntz as lastload_date,
        haulerstatementdate::timestamp_ntz as haulerstatement_date,
        runopendate::timestamp_ntz as runopen_date,
        blogicdatestamp::timestamp_ntz as blogic_date_stamp,
        allocationdatestamp::timestamp_ntz as allocation_date_stamp,
        runticketdate::timestamp_ntz as run_ticket_date,

        -- well/completion attributes
        apiversion::number as apiversion,

        -- geography
        purchaserstatementseq::number as purchaserstatementseq,
        haulerstatementseq::number as haulerstatementseq,

        -- accounting/business entities
        hauler::number as hauler,
        waterhauler::number as waterhauler,
        purchaserloc::number as purchaserloc,
        purchaser::number as purchaser,
        waterpurchaser::number as waterpurchaser,
        haulerreportedbarrels::float as haulerreportedbarrels,
        haulerloc::number as haulerloc,

        -- volumes
        grossbarrels::float as grossbarrels,
        haulerreportednetbarrels::float as haulerreportednetbarrels,
        netbarrels::float as netbarrels,

        -- temperatures
        closetemperature::float as closetemperature,
        observedtemperature::float as observedtemperature,
        opentemperature::float as opentemperature,

        -- allocation factors
        lactmeterfactor::float as lactmeterfactor,
        lactcompressibilityfactor::float as lactcompressibilityfactor,
        haulerreportedconvertedgravity::float as haulerreportedconvertedgravity,
        convertedgravity::float as convertedgravity,
        allocatedgravity::float as allocatedgravity,
        actualgravity::float as actualgravity,

        -- flags
        unitsflag::number as units_flag,
        oilwaterrunflag::number as oilwaterrun_flag,
        calculationstatusflag::number as calculationstatus_flag,
        actualestimatedflag::number as actualestimated_flag,
        allocatedticketflag::number as allocatedticket_flag,
        transferassoldflag::number as transferassold_flag,
        tankisolatedflag::number as tankisolated_flag,
        slopflag::number as slop_flag,
        primosendflag::number as primosend_flag,
        runordispositionflag::number as runordisposition_flag,
        deleteflag::number as delete_flag,
        backgroundtaskflag::number as backgroundtask_flag,
        transmitflag::number as transmit_flag,
        usehaulerbarrelsflag::number as usehaulerbarrels_flag,

        -- audit/metadata
        usernumber4::float as usernumber4,
        trim(usertimestamp)::varchar as user_timestamp,
        trim(userstringa)::varchar as userstringa,
        trim(userstringd)::varchar as userstringd,
        userid::number as user_id,
        usernumber3::float as usernumber3,
        trim(rowuid)::varchar as rowu_id,
        trim(userstringc)::varchar as userstringc,
        usernumber2::float as usernumber2,
        trim(userstringb)::varchar as userstringb,
        usernumber1::float as usernumber1,
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_tz as _fivetran_synced,

        -- other
        openinch::number as openinch,
        waxpercent::float as waxpercent,
        lactdensitycorrection::float as lactdensitycorrection,
        bottomopeninch::number as bottomopeninch,
        bottomcloseinch::number as bottomcloseinch,
        topopentotalinches::float as topopentotalinches,
        allocatedwaterbarrels::float as allocatedwaterbarrels,
        sealdistributioncode::number as sealdistribution_code,
        watercut::float as watercut,
        datasourcecode::number as datasource_code,
        sandcut::float as sandcut,
        destinationobjecttype::number as destinationobject_type,
        saltcontentppm::float as saltcontentppm,
        topclosetotalinches::float as topclosetotalinches,
        trim(blogictimestamp)::varchar as blogic_timestamp,
        trim(backsealon)::varchar as backsealon,
        trim(allocationtimestamp)::varchar as allocation_timestamp,
        bsandw::float as bsandw,
        closefeet::number as closefeet,
        sequencenumber::number as sequence_number,
        qbytetickettype::number as qbyteticket_type,
        producttype::number as product_type,
        trim(sealon)::varchar as sealon,
        trim(sealoff)::varchar as sealoff,
        bottomclosetotalinches::float as bottomclosetotalinches,
        grossngl::float as grossngl,
        trailercapacity::float as trailercapacity,
        sulfurcontentppm::float as sulfurcontentppm,
        bottomclosequarter::float as bottomclosequarter,
        originationobjecttype::number as originationobject_type,
        openodometer::float as openodometer,
        backsealdistributioncode::number as backsealdistribution_code,
        netngl::float as netngl,
        allocatedngl::float as allocatedngl,
        freewater::float as freewater,
        bottomopentotalinches::float as bottomopentotalinches,
        netwaterbarrels::float as netwaterbarrels,
        closeinch::number as closeinch,
        trim(backsealdistribution)::varchar as backsealdistribution,
        closequarter::float as closequarter,
        trim(destination)::varchar as destination,
        dispositioncode::number as disposition_code,
        productcode::number as product_code,
        bottomopenquarter::float as bottomopenquarter,
        openfeet::number as openfeet,
        openquarter::float as openquarter,
        closeodometer::float as closeodometer,
        lasttransmission::number as lasttransmission,
        emulsionpercent::float as emulsionpercent,
        bottomopenfeet::number as bottomopenfeet,
        percentfull::float as percentfull,
        allocatedoilbarrels::float as allocatedoilbarrels,
        bottomclosefeet::number as bottomclosefeet,
        trim(backsealoff)::varchar as backsealoff,
        watervaporcontentppm::float as watervaporcontentppm

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
        {{ dbt_utils.generate_surrogate_key(['merrick_id', 'record_date', 'run_ticket_number']) }} as tankrunticket_sk,
        *,
        current_timestamp() as _loaded_at

    from filtered

),

final as (

    select
        tankrunticket_sk,

        -- identifiers
        merrick_id,
        run_ticket_number,
        source_id,
        purchaserstatement_id,
        originationobject_id,
        haulertank_id,
        reidvaporpressure,
        source_type,
        destinationobject_id,
        destinationlocation_id,
        purchasertank_id,
        regulatoryfacility_id,
        haulerstatement_id,
        fluiddisposition,
        comment_id,
        sourcelocation_id,

        -- dates
        record_date,
        runclosetime,
        lastloadtime,
        date_timestamp,
        runopentime,
        runclose_date,
        user_date_stamp,
        lastload_date,
        haulerstatement_date,
        runopen_date,
        blogic_date_stamp,
        allocation_date_stamp,
        run_ticket_date,

        -- well/completion attributes
        apiversion,

        -- geography
        purchaserstatementseq,
        haulerstatementseq,

        -- accounting/business entities
        hauler,
        waterhauler,
        purchaserloc,
        purchaser,
        waterpurchaser,
        haulerreportedbarrels,
        haulerloc,

        -- volumes
        grossbarrels,
        haulerreportednetbarrels,
        netbarrels,

        -- temperatures
        closetemperature,
        observedtemperature,
        opentemperature,

        -- allocation factors
        lactmeterfactor,
        lactcompressibilityfactor,
        haulerreportedconvertedgravity,
        convertedgravity,
        allocatedgravity,
        actualgravity,

        -- flags
        units_flag,
        oilwaterrun_flag,
        calculationstatus_flag,
        actualestimated_flag,
        allocatedticket_flag,
        transferassold_flag,
        tankisolated_flag,
        slop_flag,
        primosend_flag,
        runordisposition_flag,
        delete_flag,
        backgroundtask_flag,
        transmit_flag,
        usehaulerbarrels_flag,

        -- audit/metadata
        usernumber4,
        user_timestamp,
        userstringa,
        userstringd,
        user_id,
        usernumber3,
        rowu_id,
        userstringc,
        usernumber2,
        userstringb,
        usernumber1,
        _fivetran_deleted,
        _fivetran_synced,

        -- other
        openinch,
        waxpercent,
        lactdensitycorrection,
        bottomopeninch,
        bottomcloseinch,
        topopentotalinches,
        allocatedwaterbarrels,
        sealdistribution_code,
        watercut,
        datasource_code,
        sandcut,
        destinationobject_type,
        saltcontentppm,
        topclosetotalinches,
        blogic_timestamp,
        backsealon,
        allocation_timestamp,
        bsandw,
        closefeet,
        sequence_number,
        qbyteticket_type,
        product_type,
        sealon,
        sealoff,
        bottomclosetotalinches,
        grossngl,
        trailercapacity,
        sulfurcontentppm,
        bottomclosequarter,
        originationobject_type,
        openodometer,
        backsealdistribution_code,
        netngl,
        allocatedngl,
        freewater,
        bottomopentotalinches,
        netwaterbarrels,
        closeinch,
        backsealdistribution,
        closequarter,
        destination,
        disposition_code,
        product_code,
        bottomopenquarter,
        openfeet,
        openquarter,
        closeodometer,
        lasttransmission,
        emulsionpercent,
        bottomopenfeet,
        percentfull,
        allocatedoilbarrels,
        bottomclosefeet,
        backsealoff,
        watervaporcontentppm,

        -- dbt metadata
        _loaded_at

    from enhanced

)

select * from final