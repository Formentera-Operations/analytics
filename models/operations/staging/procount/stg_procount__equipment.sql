{{
    config(
        materialized='view',
        tags=['procount', 'staging', 'crescent']
    )
}}

with

source as (

    select * from {{ source('procount', 'EQUIPMENTTB') }}

),

renamed as (

    select
        -- identifiers
        merrickid::number as merrick_id,
        gasanalysissourceid::number as gasanalysissource_id,
        engineerpersonid::number as engineerperson_id,
        plantid::number as plant_id,
        validationdaysback::number as validationdaysback,
        leaseid::number as lease_id,
        outsideoperatedflag::number as outsideoperated_flag,
        trim(engineeringid)::varchar as engineering_id,
        foremanpersonid::number as foremanperson_id,
        batteryid::number as battery_id,
        routeid::number as route_id,
        equipmentownerid::number as equipmentowner_id,
        pumperpersonid::number as pumperperson_id,
        gatheringsystemid::number as gathering_system_id,
        accountingteamid::number as accountingteam_id,
        platformid::number as platform_id,
        divisionid::number as division_id,
        productionteamid::number as productionteam_id,
        fieldgroupid::number as fieldgroup_id,
        trim(accountingid)::varchar as accounting_id,
        drillingteamid::number as drillingteam_id,
        parametertemplateid::number as parametertemplate_id,
        superpersonid::number as superperson_id,
        groupid::number as group_id,
        manufacturerbeid::number as manufacturerbe_id,
        allocationgroupid::number as allocationgroup_id,
        locationmerrickid::number as location_merrick_id,
        countyid::number as county_id,
        facilityid::number as facility_id,
        trim(scadaid)::varchar as scada_id,
        areaid::number as area_id,
        stateid::number as state_id,
        accountantpersonid::number as accountantperson_id,
        trim(productionid)::varchar as production_id,

        -- dates
        cleanupdatestamp::timestamp_ntz as cleanupdatestamp,
        dateinstalled::timestamp_ntz as dateinstalled,
        startactivedate::timestamp_ntz as startactive_date,
        dataeditstartdate::timestamp_ntz as dataeditstart_date,
        lastloaddate::timestamp_ntz as lastload_date,
        datetimestamp::timestamp_ntz as date_timestamp,
        trim(cleanuptimestamp)::varchar as cleanuptimestamp,
        trim(lastloadtime)::varchar as lastloadtime,
        allocationtypestartdate::timestamp_ntz as allocationtypestart_date,
        userdatestamp::timestamp_ntz as user_date_stamp,
        blogicdatestamp::timestamp_ntz as blogic_date_stamp,
        recordcreationdate::timestamp_ntz as recordcreation_date,
        allocautostartdate::timestamp_ntz as allocautostart_date,
        endactivedate::timestamp_ntz as endactive_date,
        rentdate::timestamp_ntz as rent_date,

        -- well/completion attributes
        completionchildcount::number as completionchildcount,
        completionmethod::number as completionmethod,

        -- names and descriptions
        trim(leasedescription)::varchar as leasedescription,
        trim(userdailynumber1label)::varchar as userdailynumber1label,
        trim(usernumber4label)::varchar as usernumber4label,
        trim(userdailynumber6label)::varchar as userdailynumber6label,
        trim(userdailystringalabel)::varchar as userdailystringalabel,
        trim(usernumber5label)::varchar as usernumber5label,
        trim(userstringalabel)::varchar as userstringalabel,
        trim(userdailynumber2label)::varchar as userdailynumber2label,
        trim(usernumber6label)::varchar as usernumber6label,
        trim(userstringblabel)::varchar as userstringblabel,
        trim(userdailystringblabel)::varchar as userdailystringblabel,
        trim(allocationdailycomment)::varchar as allocationdailycomment,
        trim(userdailynumber3label)::varchar as userdailynumber3label,
        trim(allocationmonthlycomment)::varchar as allocationmonthlycomment,
        trim(userstringclabel)::varchar as userstringclabel,
        trim(equipmentname)::varchar as equipment_name,
        trim(userdailynumber4label)::varchar as userdailynumber4label,
        trim(userstringdlabel)::varchar as userstringdlabel,
        trim(usernumber1label)::varchar as usernumber1label,
        trim(userstringelabel)::varchar as userstringelabel,
        trim(equipmentdescription)::varchar as equipmentdescription,
        trim(userdailynumber5label)::varchar as userdailynumber5label,
        trim(setupcomments)::varchar as setupcomments,
        trim(usernumber2label)::varchar as usernumber2label,
        trim(userstringflabel)::varchar as userstringflabel,
        trim(usernumber3label)::varchar as usernumber3label,

        -- geography
        trim(statepointofdisposition)::varchar as statepointofdisposition,

        -- temperatures
        templaterecordflag::number as templaterecord_flag,
        templaterecordused::number as templaterecordused,

        -- allocation factors
        leaseusecoefficienttype::number as leaseusecoefficient_type,
        leaseusecoefficient::float as leaseusecoefficient,

        -- operational/equipment
        horsepower::float as horsepower,
        trim(pumperinstructions)::varchar as pumperinstructions,
        numberofstages::number as numberofstages,
        pumpercharges::float as pumpercharges,

        -- flags
        hoursontotal24flag::number as hoursontotal24flag,
        unitstypeflag::number as unitstype_flag,
        calculationstatusflag::number as calculationstatus_flag,
        engineeringuploadflag::number as engineeringupload_flag,
        volumeautopopulateflag::number as volumeautopopulate_flag,
        printflag::number as print_flag,
        allocationtypeflag::number as allocationtype_flag,
        accountinguploadflag::number as accountingupload_flag,
        hoursondefaultflag::number as hoursondefault_flag,
        allocautoflag::number as allocauto_flag,
        activeflag::number as active_flag,
        deleteflag::number as delete_flag,
        unitsconfigurableflag::number as unitsconfigurable_flag,
        carryforwardflag::number as carryforward_flag,
        transmitflag::number as transmit_flag,
        sealrequiredflag::number as sealrequired_flag,

        -- audit/metadata
        userid::number as user_id,
        trim(userstringe)::varchar as userstringe,
        usernumber1::float as usernumber1,
        usernumber6::float as usernumber6,
        trim(userstringb)::varchar as userstringb,
        trim(usertimestamp)::varchar as user_timestamp,
        usernumber3::float as usernumber3,
        trim(userstringd)::varchar as userstringd,
        trim(rowuid)::varchar as rowu_id,
        usernumber5::float as usernumber5,
        trim(userstringa)::varchar as userstringa,
        trim(userstringf)::varchar as userstringf,
        usernumber2::float as usernumber2,
        trim(userstringc)::varchar as userstringc,
        usernumber4::float as usernumber4,
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_tz as _fivetran_synced,

        -- other
        trim(skunumber)::varchar as sku_number,
        lasttransmission::number as lasttransmission,
        trim(serialnumber)::varchar as serial_number,
        dispositioncode::number as disposition_code,
        producttype::number as product_type,
        meterchildcount::number as meterchildcount,
        trim(make)::varchar as make,
        trim(blogictimestamp)::varchar as blogic_timestamp,
        trim(equipmentnumber)::varchar as equipment_number,
        trim(systemmessage)::varchar as systemmessage,
        trim(internalunitnumber)::varchar as internalunit_number,
        trim(modelnumber)::varchar as model_number,
        trim(agreementnumber)::varchar as agreement_number,
        allocationruntwicemonthly::number as allocationruntwicemonthly,
        equipmentownerloc::number as equipmentownerloc,
        maintenancecharges::float as maintenancecharges,
        allocationruntwicedaily::number as allocationruntwicedaily,
        allocationorder::number as allocationorder,
        trim(rentalcompanyunitnumber)::varchar as rentalcompanyunit_number,
        allocationbycomponent::number as allocationbycomponent,
        gasanalysissourcetype::number as gasanalysissource_type,
        insurancecharges::float as insurancecharges,
        rentaltermsmonths::number as rentaltermsmonths,
        productcode::number as product_code,
        measurementpointrole::number as measurementpointrole,
        baserentalcharges::float as baserentalcharges,
        purchaseoption::number as purchaseoption,
        equipmenttype::number as equipment_type,
        locationorder::number as locationorder

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
        gasanalysissource_id,
        engineerperson_id,
        plant_id,
        validationdaysback,
        lease_id,
        outsideoperated_flag,
        engineering_id,
        foremanperson_id,
        battery_id,
        route_id,
        equipmentowner_id,
        pumperperson_id,
        gathering_system_id,
        accountingteam_id,
        platform_id,
        division_id,
        productionteam_id,
        fieldgroup_id,
        accounting_id,
        drillingteam_id,
        parametertemplate_id,
        superperson_id,
        group_id,
        manufacturerbe_id,
        allocationgroup_id,
        location_merrick_id,
        county_id,
        facility_id,
        scada_id,
        area_id,
        state_id,
        accountantperson_id,
        production_id,

        -- dates
        cleanupdatestamp,
        dateinstalled,
        startactive_date,
        dataeditstart_date,
        lastload_date,
        date_timestamp,
        cleanuptimestamp,
        lastloadtime,
        allocationtypestart_date,
        user_date_stamp,
        blogic_date_stamp,
        recordcreation_date,
        allocautostart_date,
        endactive_date,
        rent_date,

        -- well/completion attributes
        completionchildcount,
        completionmethod,

        -- names and descriptions
        leasedescription,
        userdailynumber1label,
        usernumber4label,
        userdailynumber6label,
        userdailystringalabel,
        usernumber5label,
        userstringalabel,
        userdailynumber2label,
        usernumber6label,
        userstringblabel,
        userdailystringblabel,
        allocationdailycomment,
        userdailynumber3label,
        allocationmonthlycomment,
        userstringclabel,
        equipment_name,
        userdailynumber4label,
        userstringdlabel,
        usernumber1label,
        userstringelabel,
        equipmentdescription,
        userdailynumber5label,
        setupcomments,
        usernumber2label,
        userstringflabel,
        usernumber3label,

        -- geography
        statepointofdisposition,

        -- temperatures
        templaterecord_flag,
        templaterecordused,

        -- allocation factors
        leaseusecoefficient_type,
        leaseusecoefficient,

        -- operational/equipment
        horsepower,
        pumperinstructions,
        numberofstages,
        pumpercharges,

        -- flags
        hoursontotal24flag,
        unitstype_flag,
        calculationstatus_flag,
        engineeringupload_flag,
        volumeautopopulate_flag,
        print_flag,
        allocationtype_flag,
        accountingupload_flag,
        hoursondefault_flag,
        allocauto_flag,
        active_flag,
        delete_flag,
        unitsconfigurable_flag,
        carryforward_flag,
        transmit_flag,
        sealrequired_flag,

        -- audit/metadata
        user_id,
        userstringe,
        usernumber1,
        usernumber6,
        userstringb,
        user_timestamp,
        usernumber3,
        userstringd,
        rowu_id,
        usernumber5,
        userstringa,
        userstringf,
        usernumber2,
        userstringc,
        usernumber4,
        _fivetran_deleted,
        _fivetran_synced,

        -- other
        sku_number,
        lasttransmission,
        serial_number,
        disposition_code,
        product_type,
        meterchildcount,
        make,
        blogic_timestamp,
        equipment_number,
        systemmessage,
        internalunit_number,
        model_number,
        agreement_number,
        allocationruntwicemonthly,
        equipmentownerloc,
        maintenancecharges,
        allocationruntwicedaily,
        allocationorder,
        rentalcompanyunit_number,
        allocationbycomponent,
        gasanalysissource_type,
        insurancecharges,
        rentaltermsmonths,
        product_code,
        measurementpointrole,
        baserentalcharges,
        purchaseoption,
        equipment_type,
        locationorder,

        -- dbt metadata
        _loaded_at

    from enhanced

)

select * from final