{{
    config(
        materialized='view',
        tags=['procount', 'staging', 'crescent']
    )
}}

with

source as (

    select * from {{ source('procount', 'TANKTB') }}

),

renamed as (

    select
        -- identifiers
        merrickid::number as merrick_id,
        areaid::number as area_id,
        pumperpersonid::number as pumperperson_id,
        platformid::number as platform_id,
        engineerpersonid::number as engineerperson_id,
        operatorentityid::number as operatorentity_id,
        plantid::number as plant_id,
        accountingteamid::number as accountingteam_id,
        productionteamid::number as productionteam_id,
        trim(productionid)::varchar as production_id,
        haulerbeid::number as haulerbe_id,
        parametertemplateid::number as parametertemplate_id,
        locationmerrickid::number as location_merrick_id,
        divisionid::number as division_id,
        batteryid::number as battery_id,
        skipmidstreamcalc::number as skipmidstreamcalc,
        validationdaysback::number as validationdaysback,
        routeid::number as route_id,
        countyid::number as county_id,
        allocationgroupid::number as allocationgroup_id,
        superpersonid::number as superperson_id,
        trim(accountingid)::varchar as accounting_id,
        foremanpersonid::number as foremanperson_id,
        gatheringsystemid::number as gathering_system_id,
        manufacturerbeid::number as manufacturerbe_id,
        terminalid::number as terminal_id,
        tankbatteryid::number as tankbattery_id,
        trim(purchasertankid)::varchar as purchasertank_id,
        drillingteamid::number as drillingteam_id,
        pipelineid::number as pipeline_id,
        facilityid::number as facility_id,
        outsideoperatedflag::number as outsideoperated_flag,
        stateid::number as state_id,
        fieldgroupid::number as fieldgroup_id,
        gasanalysissourceid::number as gasanalysissource_id,
        leaseid::number as lease_id,
        trim(scadaid)::varchar as scada_id,
        trim(engineeringid)::varchar as engineering_id,
        purchaserbeid::number as purchaserbe_id,
        groupid::number as group_id,
        mastertankbatteryid::number as mastertankbattery_id,
        accountantpersonid::number as accountantperson_id,

        -- dates
        lastloaddate::timestamp_ntz as lastload_date,
        endactivedate::timestamp_ntz as endactive_date,
        userdatestamp::timestamp_ntz as user_date_stamp,
        laststrappingdate::timestamp_ntz as laststrapping_date,
        enddate::timestamp_ntz as end_date,
        recordcreationdate::timestamp_ntz as recordcreation_date,
        startdate::timestamp_ntz as start_date,
        datetimestamp::timestamp_ntz as date_timestamp,
        startactivedate::timestamp_ntz as startactive_date,
        trim(cleanuptimestamp)::varchar as cleanuptimestamp,
        dataeditstartdate::timestamp_ntz as dataeditstart_date,
        allocationtypestartdate::timestamp_ntz as allocationtypestart_date,
        blogicdatestamp::timestamp_ntz as blogic_date_stamp,
        cleanupdatestamp::timestamp_ntz as cleanupdatestamp,
        trim(standardgaugetime)::varchar as standardgaugetime,
        allocautostartdate::timestamp_ntz as allocautostart_date,
        trim(lastloadtime)::varchar as lastloadtime,

        -- well/completion attributes
        completionchildcount::number as completionchildcount,

        -- names and descriptions
        trim(userstringclabel)::varchar as userstringclabel,
        trim(usernumber5label)::varchar as usernumber5label,
        trim(allocationmonthlycomment)::varchar as allocationmonthlycomment,
        trim(userstringalabel)::varchar as userstringalabel,
        trim(usernumber3label)::varchar as usernumber3label,
        trim(userstringflabel)::varchar as userstringflabel,
        trim(usernumber1label)::varchar as usernumber1label,
        trim(tankname)::varchar as tank_name,
        trim(userstringdlabel)::varchar as userstringdlabel,
        trim(usernumber6label)::varchar as usernumber6label,
        trim(setupcomments)::varchar as setupcomments,
        trim(userstringblabel)::varchar as userstringblabel,
        trim(usernumber4label)::varchar as usernumber4label,
        trim(usernumber2label)::varchar as usernumber2label,
        trim(allocationdailycomment)::varchar as allocationdailycomment,
        trim(tankdescription)::varchar as tankdescription,
        trim(userstringelabel)::varchar as userstringelabel,

        -- geography
        trim(statepointofdisposition)::varchar as statepointofdisposition,
        trim(statepointofdispositionoil)::varchar as statepointofdispositionoil,
        trim(statepointofdispositionwater)::varchar as statepointofdispositionwater,
        trim(latitude)::varchar as latitude,
        trim(longitude)::varchar as longitude,
        tankbatteryrole::number as tankbatteryrole,

        -- accounting/business entities
        trim(operatortanknumber)::varchar as operatortank_number,
        operatorentityloc::number as operatorentityloc,
        trim(purchaserlocationnumber)::varchar as purchaserlocation_number,
        trim(haulertanknumber)::varchar as haulertank_number,
        purchaserbeloc::number as purchaserbeloc,
        waterhauler::number as waterhauler,
        waterpurchaser::number as waterpurchaser,
        haulerbeloc::number as haulerbeloc,

        -- temperatures
        shellreferencetemperature::float as shellreferencetemperature,

        -- allocation factors
        shellexpansioncoefficient::float as shellexpansioncoefficient,

        -- operational/equipment
        trim(pumperinstructions)::varchar as pumperinstructions,

        -- flags
        fifoallocationflag::number as fifoallocation_flag,
        transmitflag::number as transmit_flag,
        ignorebswvolumeflag::number as ignorebswvolume_flag,
        unitstypeflag::number as unitstype_flag,
        engineeringuploadflag::number as engineeringupload_flag,
        unitsconfigurableflag::number as unitsconfigurable_flag,
        printflag::number as print_flag,
        allocationtypeflag::number as allocationtype_flag,
        calculationstatusflag::number as calculationstatus_flag,
        deleteflag::number as delete_flag,
        activeflag::number as active_flag,
        includeinventoryinregsflag::number as includeinventoryinregs_flag,
        insulatedflag::number as insulated_flag,
        usevaporadjustmentflag::number as usevaporadjustment_flag,
        accountinguploadflag::number as accountingupload_flag,
        carryforwardflag::number as carryforward_flag,
        sealrequiredflag::number as sealrequired_flag,
        allocautoflag::number as allocauto_flag,

        -- audit/metadata
        userid::number as user_id,
        usernumber5::float as usernumber5,
        trim(usertimestamp)::varchar as user_timestamp,
        trim(userstringb)::varchar as userstringb,
        trim(rowuid)::varchar as rowu_id,
        trim(userstringe)::varchar as userstringe,
        usernumber1::float as usernumber1,
        usernumber4::float as usernumber4,
        trim(userstringa)::varchar as userstringa,
        trim(userstringd)::varchar as userstringd,
        usernumber3::float as usernumber3,
        usernumber6::float as usernumber6,
        trim(userstringc)::varchar as userstringc,
        trim(userstringf)::varchar as userstringf,
        usernumber2::float as usernumber2,
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_tz as _fivetran_synced,

        -- other
        watercutaveragedays::number as watercutaveragedays,
        barrelsperquarterinch::float as barrelsperquarterinch,
        trim(serialnumber)::varchar as serial_number,
        trim(make)::varchar as make,
        tanktype::number as tank_type,
        trim(mmsmeteringpoint)::varchar as mmsmeteringpoint,
        tankconstruction::number as tankconstruction,
        allocationruntwicemonthly::number as allocationruntwicemonthly,
        lasttransmission::number as lasttransmission,
        barrelsperinch::float as barrelsperinch,
        topfeet::number as topfeet,
        measurementpointrole::number as measurementpointrole,
        topquarter::float as topquarter,
        trim(blogictimestamp)::varchar as blogic_timestamp,
        trim(systemmessage)::varchar as systemmessage,
        producttype::number as product_type,
        locationorder::number as locationorder,
        trim(mmsfmpnumber)::varchar as mmsfmp_number,
        allocationorder::number as allocationorder,
        tanksize::float as tanksize,
        trim(modelnumber)::varchar as model_number,
        toptotalinches::float as toptotalinches,
        topinches::number as topinches,
        tankgaugetype::number as tankgauge_type,
        productcode::number as product_code,
        gasanalysissourcetype::number as gasanalysissource_type,
        allocationruntwicedaily::number as allocationruntwicedaily

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
        *,
        current_timestamp() as _loaded_at

    from filtered

),

final as (

    select
        -- identifiers
        merrick_id,
        area_id,
        pumperperson_id,
        platform_id,
        engineerperson_id,
        operatorentity_id,
        plant_id,
        accountingteam_id,
        productionteam_id,
        production_id,
        haulerbe_id,
        parametertemplate_id,
        location_merrick_id,
        division_id,
        battery_id,
        skipmidstreamcalc,
        validationdaysback,
        route_id,
        county_id,
        allocationgroup_id,
        superperson_id,
        accounting_id,
        foremanperson_id,
        gathering_system_id,
        manufacturerbe_id,
        terminal_id,
        tankbattery_id,
        purchasertank_id,
        drillingteam_id,
        pipeline_id,
        facility_id,
        outsideoperated_flag,
        state_id,
        fieldgroup_id,
        gasanalysissource_id,
        lease_id,
        scada_id,
        engineering_id,
        purchaserbe_id,
        group_id,
        mastertankbattery_id,
        accountantperson_id,

        -- dates
        lastload_date,
        endactive_date,
        user_date_stamp,
        laststrapping_date,
        end_date,
        recordcreation_date,
        start_date,
        date_timestamp,
        startactive_date,
        cleanuptimestamp,
        dataeditstart_date,
        allocationtypestart_date,
        blogic_date_stamp,
        cleanupdatestamp,
        standardgaugetime,
        allocautostart_date,
        lastloadtime,

        -- well/completion attributes
        completionchildcount,

        -- names and descriptions
        userstringclabel,
        usernumber5label,
        allocationmonthlycomment,
        userstringalabel,
        usernumber3label,
        userstringflabel,
        usernumber1label,
        tank_name,
        userstringdlabel,
        usernumber6label,
        setupcomments,
        userstringblabel,
        usernumber4label,
        usernumber2label,
        allocationdailycomment,
        tankdescription,
        userstringelabel,

        -- geography
        statepointofdisposition,
        statepointofdispositionoil,
        statepointofdispositionwater,
        latitude,
        longitude,
        tankbatteryrole,

        -- accounting/business entities
        operatortank_number,
        operatorentityloc,
        purchaserlocation_number,
        haulertank_number,
        purchaserbeloc,
        waterhauler,
        waterpurchaser,
        haulerbeloc,

        -- temperatures
        shellreferencetemperature,

        -- allocation factors
        shellexpansioncoefficient,

        -- operational/equipment
        pumperinstructions,

        -- flags
        fifoallocation_flag,
        transmit_flag,
        ignorebswvolume_flag,
        unitstype_flag,
        engineeringupload_flag,
        unitsconfigurable_flag,
        print_flag,
        allocationtype_flag,
        calculationstatus_flag,
        delete_flag,
        active_flag,
        includeinventoryinregs_flag,
        insulated_flag,
        usevaporadjustment_flag,
        accountingupload_flag,
        carryforward_flag,
        sealrequired_flag,
        allocauto_flag,

        -- audit/metadata
        user_id,
        usernumber5,
        user_timestamp,
        userstringb,
        rowu_id,
        userstringe,
        usernumber1,
        usernumber4,
        userstringa,
        userstringd,
        usernumber3,
        usernumber6,
        userstringc,
        userstringf,
        usernumber2,
        _fivetran_deleted,
        _fivetran_synced,

        -- other
        watercutaveragedays,
        barrelsperquarterinch,
        serial_number,
        make,
        tank_type,
        mmsmeteringpoint,
        tankconstruction,
        allocationruntwicemonthly,
        lasttransmission,
        barrelsperinch,
        topfeet,
        measurementpointrole,
        topquarter,
        blogic_timestamp,
        systemmessage,
        product_type,
        locationorder,
        mmsfmp_number,
        allocationorder,
        tanksize,
        model_number,
        toptotalinches,
        topinches,
        tankgauge_type,
        product_code,
        gasanalysissource_type,
        allocationruntwicedaily,

        -- dbt metadata
        _loaded_at

    from enhanced

)

select * from final