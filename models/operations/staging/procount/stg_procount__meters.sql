{{
    config(
        materialized='view',
        tags=['procount', 'staging', 'crescent']
    )
}}

with

source as (

    select * from {{ source('procount', 'METERTB') }}

),

renamed as (

    select
        -- identifiers
        merrickid::number as merrick_id,
        trim(integratormeterid)::varchar as integratormeter_id,
        batteryid::number as battery_id,
        routeid::number as route_id,
        plantid::number as plant_id,
        drillingteamid::number as drillingteam_id,
        trim(scadaid)::varchar as scada_id,
        trim(engineeringid)::varchar as engineering_id,
        areaid::number as area_id,
        trim(regulatoryfacilityid)::varchar as regulatoryfacility_id,
        foremanpersonid::number as foremanperson_id,
        manufacturerbeid::number as manufacturerbe_id,
        gasanalysissourceid::number as gasanalysissource_id,
        meteroperatorbeid::number as meteroperatorbe_id,
        trim(productionid)::varchar as production_id,
        platformid::number as platform_id,
        trim(accountingid)::varchar as accounting_id,
        pipelineid::number as pipeline_id,
        groupid::number as group_id,
        checkmetermerrickid::number as checkmetermerrick_id,
        trim(locationid)::varchar as location_id,
        gatheringsystemid::number as gathering_system_id,
        allocationgroupid::number as allocationgroup_id,
        trim(regulatoryfacilitylongid)::varchar as regulatoryfacilitylong_id,
        superpersonid::number as superperson_id,
        trim(accountingtransportmeterid)::varchar as accountingtransportmeter_id,
        trim(gathererid)::varchar as gatherer_id,
        trim(standardmeterid)::varchar as standardmeter_id,
        locationmerrickid::number as location_merrick_id,
        accountantpersonid::number as accountantperson_id,
        facilityid::number as facility_id,
        countyid::number as county_id,
        accountingteamid::number as accountingteam_id,
        purchaserbeid::number as purchaserbe_id,
        terminalid::number as terminal_id,
        trim(operatormeterid)::varchar as operatormeter_id,
        leaseid::number as lease_id,
        pumperpersonid::number as pumperperson_id,
        stateid::number as state_id,
        trim(transporterid)::varchar as transporter_id,
        parametertemplateid::number as parametertemplate_id,
        engineerpersonid::number as engineerperson_id,
        trim(purchasermeterid)::varchar as purchasermeter_id,
        gathererbeid::number as gathererbe_id,
        validationdaysback::number as validationdaysback,
        integratorbeid::number as integratorbe_id,
        transporterbeid::number as transporterbe_id,
        productionteamid::number as productionteam_id,
        fieldgroupid::number as fieldgroup_id,
        divisionid::number as division_id,
        regulatoryfieldid::number as regulatoryfield_id,
        outsideoperatedflag::number as outsideoperated_flag,

        -- dates
        trim(standardreadingtime)::varchar as standardreadingtime,
        startdate::timestamp_ntz as start_date,
        lastloaddate::timestamp_ntz as lastload_date,
        trim(cleanuptimestamp)::varchar as cleanuptimestamp,
        allocautostartdate::timestamp_ntz as allocautostart_date,
        startactivedate::timestamp_ntz as startactive_date,
        datetimestamp::timestamp_ntz as date_timestamp,
        enddate::timestamp_ntz as end_date,
        trim(lastloadtime)::varchar as lastloadtime,
        allocationtypestartdate::timestamp_ntz as allocationtypestart_date,
        endactivedate::timestamp_ntz as endactive_date,
        cleanupdatestamp::timestamp_ntz as cleanupdatestamp,
        dataeditstartdate::timestamp_ntz as dataeditstart_date,
        blogicdatestamp::timestamp_ntz as blogic_date_stamp,
        userdatestamp::timestamp_ntz as user_date_stamp,
        recordcreationdate::timestamp_ntz as recordcreation_date,

        -- names and descriptions
        trim(userstringflabel)::varchar as userstringflabel,
        trim(usernumber6label)::varchar as usernumber6label,
        trim(meterdescription)::varchar as meterdescription,
        trim(setupcomments)::varchar as setupcomments,
        trim(allocationdailycomment)::varchar as allocationdailycomment,
        trim(allocationmonthlycomment)::varchar as allocationmonthlycomment,
        trim(allocationreportcomment)::varchar as allocationreportcomment,
        trim(userstringblabel)::varchar as userstringblabel,
        trim(userstringclabel)::varchar as userstringclabel,
        trim(userstringalabel)::varchar as userstringalabel,
        trim(usernumber1label)::varchar as usernumber1label,
        trim(usernumber4label)::varchar as usernumber4label,
        trim(userstringdlabel)::varchar as userstringdlabel,
        trim(usernumber2label)::varchar as usernumber2label,
        trim(legaldescription)::varchar as legaldescription,
        trim(userstringelabel)::varchar as userstringelabel,
        trim(usernumber5label)::varchar as usernumber5label,
        trim(usernumber3label)::varchar as usernumber3label,
        trim(metername)::varchar as meter_name,

        -- geography
        trim(statepointofdisposition)::varchar as statepointofdisposition,
        trim(latitude)::varchar as latitude,
        trim(regulatoryfacilitytype)::varchar as regulatoryfacility_type,
        trim(regionarea)::varchar as regionarea,
        trim(longitude)::varchar as longitude,
        trim(stateplantnumber)::varchar as stateplant_number,

        -- accounting/business entities
        purchaserbeloc::number as purchaserbeloc,
        gathererbeloc::number as gathererbeloc,
        transporterbeloc::number as transporterbeloc,
        meteroperatorbeloc::number as meteroperatorbeloc,

        -- pressures
        absolutepressure::float as absolutepressure,

        -- temperatures
        templaterecordused::number as templaterecordused,
        templaterecordflag::number as templaterecord_flag,

        -- allocation factors
        computefactorflag::number as computefactor_flag,
        fuelfactorflag::number as fuelfactor_flag,

        -- operational/equipment
        trim(pumperinstructions)::varchar as pumperinstructions,

        -- flags
        monthlydatasourceflag::number as monthlydatasource_flag,
        engineeringuploadflag::number as engineeringupload_flag,
        activeflag::number as active_flag,
        prorationcalcflag::number as prorationcalc_flag,
        sealrequiredflag::number as sealrequired_flag,
        metercalculationflag::number as metercalculation_flag,
        ignorebswvolumeflag::number as ignorebswvolume_flag,
        transmitflag::number as transmit_flag,
        copyrunticketvolumeflag::number as copyrunticketvolume_flag,
        printflag::number as print_flag,
        hoursondefaultflag::number as hoursondefault_flag,
        accountinguploadflag::number as accountingupload_flag,
        allocateusingfixedvolflag::number as allocateusingfixedvol_flag,
        statefilingflag::number as statefiling_flag,
        unitstypeflag::number as unitstype_flag,
        allocationtypeflag::number as allocationtype_flag,
        allocautoflag::number as allocauto_flag,
        calculationstatusflag::number as calculationstatus_flag,
        copyconvertvolumeflag::number as copyconvertvolume_flag,
        deleteflag::number as delete_flag,
        carryforwardflag::number as carryforward_flag,
        hoursontotal24flag::number as hoursontotal24flag,
        unitsconfigurableflag::number as unitsconfigurable_flag,
        btucopyflag::number as btucopy_flag,

        -- audit/metadata
        trim(userstringe)::varchar as userstringe,
        usernumber1::float as usernumber1,
        usernumber4::float as usernumber4,
        trim(userstringa)::varchar as userstringa,
        trim(userstringd)::varchar as userstringd,
        trim(rowuid)::varchar as rowu_id,
        usernumber3::float as usernumber3,
        trim(usertimestamp)::varchar as user_timestamp,
        usernumber6::float as usernumber6,
        trim(userstringc)::varchar as userstringc,
        trim(userstringf)::varchar as userstringf,
        usernumber2::float as usernumber2,
        usernumber5::float as usernumber5,
        userid::number as user_id,
        trim(userstringb)::varchar as userstringb,
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_tz as _fivetran_synced,

        -- other
        trim(mmsgasplantnumber)::varchar as mmsgasplant_number,
        gasanalysissourcetype::number as gasanalysissource_type,
        integratorbeloc::number as integratorbeloc,
        trim(make)::varchar as make,
        lasttransmission::number as lasttransmission,
        trim(township)::varchar as township,
        trim(meternumber)::varchar as meter_number,
        producttype::number as product_type,
        allocationorder::number as allocationorder,
        meterchildcount::number as meterchildcount,
        trim(internalreferencenumber)::varchar as internalreference_number,
        metertestfrequency::number as metertestfrequency,
        trim(range)::varchar as range,
        metertype::number as meter_type,
        trim(blogictimestamp)::varchar as blogic_timestamp,
        odometermaximum::float as odometermaximum,
        allocationbycomponent::number as allocationbycomponent,
        measurementpointrole::number as measurementpointrole,
        trim(survey)::varchar as survey,
        allocationruntwicedaily::number as allocationruntwicedaily,
        dispositioncode::number as disposition_code,
        trim(systemmessage)::varchar as systemmessage,
        trim(block)::varchar as block,
        trim(modelnumber)::varchar as model_number,
        trim(mmsfmpnumber)::varchar as mmsfmp_number,
        locationorder::number as locationorder,
        trim(section)::varchar as section,
        allocationruntwicemonthly::number as allocationruntwicemonthly,
        productcode::number as product_code,
        trim(serialnumber)::varchar as serial_number,
        trim(skunumber)::varchar as sku_number

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
        integratormeter_id,
        battery_id,
        route_id,
        plant_id,
        drillingteam_id,
        scada_id,
        engineering_id,
        area_id,
        regulatoryfacility_id,
        foremanperson_id,
        manufacturerbe_id,
        gasanalysissource_id,
        meteroperatorbe_id,
        production_id,
        platform_id,
        accounting_id,
        pipeline_id,
        group_id,
        checkmetermerrick_id,
        location_id,
        gathering_system_id,
        allocationgroup_id,
        regulatoryfacilitylong_id,
        superperson_id,
        accountingtransportmeter_id,
        gatherer_id,
        standardmeter_id,
        location_merrick_id,
        accountantperson_id,
        facility_id,
        county_id,
        accountingteam_id,
        purchaserbe_id,
        terminal_id,
        operatormeter_id,
        lease_id,
        pumperperson_id,
        state_id,
        transporter_id,
        parametertemplate_id,
        engineerperson_id,
        purchasermeter_id,
        gathererbe_id,
        validationdaysback,
        integratorbe_id,
        transporterbe_id,
        productionteam_id,
        fieldgroup_id,
        division_id,
        regulatoryfield_id,
        outsideoperated_flag,

        -- dates
        standardreadingtime,
        start_date,
        lastload_date,
        cleanuptimestamp,
        allocautostart_date,
        startactive_date,
        date_timestamp,
        end_date,
        lastloadtime,
        allocationtypestart_date,
        endactive_date,
        cleanupdatestamp,
        dataeditstart_date,
        blogic_date_stamp,
        user_date_stamp,
        recordcreation_date,

        -- names and descriptions
        userstringflabel,
        usernumber6label,
        meterdescription,
        setupcomments,
        allocationdailycomment,
        allocationmonthlycomment,
        allocationreportcomment,
        userstringblabel,
        userstringclabel,
        userstringalabel,
        usernumber1label,
        usernumber4label,
        userstringdlabel,
        usernumber2label,
        legaldescription,
        userstringelabel,
        usernumber5label,
        usernumber3label,
        meter_name,

        -- geography
        statepointofdisposition,
        latitude,
        regulatoryfacility_type,
        regionarea,
        longitude,
        stateplant_number,

        -- accounting/business entities
        purchaserbeloc,
        gathererbeloc,
        transporterbeloc,
        meteroperatorbeloc,

        -- pressures
        absolutepressure,

        -- temperatures
        templaterecordused,
        templaterecord_flag,

        -- allocation factors
        computefactor_flag,
        fuelfactor_flag,

        -- operational/equipment
        pumperinstructions,

        -- flags
        monthlydatasource_flag,
        engineeringupload_flag,
        active_flag,
        prorationcalc_flag,
        sealrequired_flag,
        metercalculation_flag,
        ignorebswvolume_flag,
        transmit_flag,
        copyrunticketvolume_flag,
        print_flag,
        hoursondefault_flag,
        accountingupload_flag,
        allocateusingfixedvol_flag,
        statefiling_flag,
        unitstype_flag,
        allocationtype_flag,
        allocauto_flag,
        calculationstatus_flag,
        copyconvertvolume_flag,
        delete_flag,
        carryforward_flag,
        hoursontotal24flag,
        unitsconfigurable_flag,
        btucopy_flag,

        -- audit/metadata
        userstringe,
        usernumber1,
        usernumber4,
        userstringa,
        userstringd,
        rowu_id,
        usernumber3,
        user_timestamp,
        usernumber6,
        userstringc,
        userstringf,
        usernumber2,
        usernumber5,
        user_id,
        userstringb,
        _fivetran_deleted,
        _fivetran_synced,

        -- other
        mmsgasplant_number,
        gasanalysissource_type,
        integratorbeloc,
        make,
        lasttransmission,
        township,
        meter_number,
        product_type,
        allocationorder,
        meterchildcount,
        internalreference_number,
        metertestfrequency,
        range,
        meter_type,
        blogic_timestamp,
        odometermaximum,
        allocationbycomponent,
        measurementpointrole,
        survey,
        allocationruntwicedaily,
        disposition_code,
        systemmessage,
        block,
        model_number,
        mmsfmp_number,
        locationorder,
        section,
        allocationruntwicemonthly,
        product_code,
        serial_number,
        sku_number,

        -- dbt metadata
        _loaded_at

    from enhanced

)

select * from final