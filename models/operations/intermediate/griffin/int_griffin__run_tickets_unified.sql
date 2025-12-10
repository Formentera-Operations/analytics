{{
    config(
        materialized='view',
        tags=['griffin', 'intermediate', 'crescent']
    )
}}

/*
    Intermediate model unifying tank and meter run tickets.
    Provides single view of all hauling/sales tickets across equipment types.
*/

with

tank_tickets as (

    select
        tankrunticket_sk as run_ticket_sk,
        merrick_id,
        record_date,
        run_ticket_number,
        'TANK' as equipment_type,
        
        -- ticket details
        run_ticket_date as ticket_date,
        runopentime as ticket_time,
        runopen_date,
        runopentime,
        runclose_date,
        runclosetime,
        
        -- purchaser/hauler
        purchaser as purchaser_id,
        purchaserloc,
        hauler as hauler_id,
        haulerloc,
        waterpurchaser,
        waterhauler,
        
        -- volumes
        grossbarrels as gross_volume,
        netbarrels as net_volume,
        haulerreportedbarrels,
        haulerreportednetbarrels,
        allocatedoilbarrels,
        allocatedwaterbarrels,
        allocatedngl,
        netwaterbarrels,
        grossngl,
        netngl,
        
        -- quality
        bsandw as bs_and_w,
        actualgravity as gravity,
        convertedgravity,
        allocatedgravity,
        observedtemperature as temperature,
        opentemperature,
        closetemperature,
        watercut,
        sandcut,
        freewater,
        emulsionpercent,
        waxpercent,
        saltcontentppm,
        sulfurcontentppm,
        
        -- correction factors
        lactmeterfactor,
        lactcompressibilityfactor,
        lactdensitycorrection,
        
        -- disposition
        disposition_code,
        product_code,
        product_type,
        fluiddisposition,
        
        -- gauge readings - top
        openfeet,
        openinch,
        openquarter,
        topopentotalinches,
        closefeet,
        closeinch,
        closequarter,
        topclosetotalinches,
        
        -- gauge readings - bottom
        bottomopenfeet,
        bottomopeninch,
        bottomopenquarter,
        bottomopentotalinches,
        bottomclosefeet,
        bottomcloseinch,
        bottomclosequarter,
        bottomclosetotalinches,
        
        -- odometer
        openodometer,
        closeodometer,
        
        -- seals
        sealon,
        sealoff,
        backsealon,
        backsealoff,
        sealdistribution_code,
        backsealdistribution_code,
        backsealdistribution,
        
        -- other
        destination,
        sequence_number,
        trailercapacity,
        percentfull,
        apiversion,
        
        -- flags
        actualestimated_flag,
        delete_flag,
        transmit_flag,
        backgroundtask_flag,
        usehaulerbarrels_flag,
        runordisposition_flag,
        oilwaterrun_flag,
        allocatedticket_flag,
        transferassold_flag,
        tankisolated_flag,
        slop_flag,
        units_flag,
        calculationstatus_flag,
        
        -- statements
        purchaserstatement_id,
        purchaserstatementseq,
        haulerstatement_id,
        haulerstatementseq,
        haulerstatement_date,
        
        -- source/destination
        source_id,
        source_type,
        sourcelocation_id,
        originationobject_id,
        originationobject_type,
        destinationobject_id,
        destinationobject_type,
        destinationlocation_id,
        
        -- metadata
        _fivetran_synced,
        _loaded_at
        
    from {{ ref('stg_procount__tank_run_tickets') }}

),

meter_tickets as (

    select
        meterrunticket_sk as run_ticket_sk,
        merrick_id,
        record_date,
        run_ticket_number,
        'METER' as equipment_type,
        
        -- ticket details
        run_ticket_date as ticket_date,
        runopentime as ticket_time,
        runopen_date,
        runopentime,
        runclose_date,
        runclosetime,
        
        -- purchaser/hauler
        purchaser as purchaser_id,
        purchaserloc,
        hauler as hauler_id,
        haulerloc,
        null as waterpurchaser,
        null as waterhauler,
        
        -- volumes
        grossbarrels as gross_volume,
        netbarrels as net_volume,
        haulerreportedbarrels,
        haulerreportednetbarrels,
        allocatedoilbarrels,
        allocatedwaterbarrels,
        allocatedngl,
        netwaterbarrels,
        grossngl,
        netngl,
        
        -- quality
        bsandw as bs_and_w,
        actualgravity as gravity,
        convertedgravity,
        allocatedgravity,
        observedtemperature as temperature,
        opentemperature,
        closetemperature,
        null as watercut,
        null as sandcut,
        null as freewater,
        emulsionpercent,
        waxpercent,
        saltcontentppm,
        sulfurcontentppm,
        
        -- correction factors
        lactmeterfactor,
        lactcompressibilityfactor,
        lactdensitycorrection,
        
        -- disposition
        disposition_code,
        product_code,
        product_type,
        fluiddisposition,
        
        -- gauge readings - not applicable for meters
        null as openfeet,
        null as openinch,
        null as openquarter,
        null as topopentotalinches,
        null as closefeet,
        null as closeinch,
        null as closequarter,
        null as topclosetotalinches,
        null as bottomopenfeet,
        null as bottomopeninch,
        null as bottomopenquarter,
        null as bottomopentotalinches,
        null as bottomclosefeet,
        null as bottomcloseinch,
        null as bottomclosequarter,
        null as bottomclosetotalinches,
        
        -- odometer
        openodometer,
        closeodometer,
        
        -- seals - not applicable for meters
        null as sealon,
        null as sealoff,
        null as backsealon,
        null as backsealoff,
        null as sealdistribution_code,
        null as backsealdistribution_code,
        null as backsealdistribution,
        
        -- other
        destination,
        null as sequence_number,
        null as trailercapacity,
        null as percentfull,
        apiversion,
        
        -- flags
        actualestimated_flag,
        delete_flag,
        transmit_flag,
        backgroundtask_flag,
        usehaulerbarrels_flag,
        runordisposition_flag,
        null as oilwaterrun_flag,
        null as allocatedticket_flag,
        null as transferassold_flag,
        null as tankisolated_flag,
        null as slop_flag,
        units_flag,
        calculationstatus_flag,
        
        -- statements
        purchaserstatement_id,
        purchaserstatementseq,
        haulerstatement_id,
        haulerstatementseq,
        haulerstatement_date,
        
        -- source/destination
        source_id,
        source_type,
        null as sourcelocation_id,
        originationobject_id,
        originationobject_type,
        destinationobject_id,
        destinationobject_type,
        null as destinationlocation_id,
        
        -- metadata
        _fivetran_synced,
        _loaded_at
        
    from {{ ref('stg_procount__meter_run_tickets') }}

),

unioned as (

    select * from tank_tickets
    union all
    select * from meter_tickets

),

final as (

    select * from unioned

)

select * from final
