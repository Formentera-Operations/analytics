{{
    config(
        materialized='view',
        tags=['procount', 'staging', 'crescent']
    )
}}

with

source as (

    select * from {{ source('procount', 'TANKMONTHLYTB') }}

),

renamed as (

    select
        -- identifiers
        merrickid::number as merrick_id,
        fieldgroupid::number as fieldgroup_id,
        accountantpersonid::number as accountantperson_id,
        productionteamid::number as productionteam_id,
        superpersonid::number as superperson_id,
        foremanpersonid::number as foremanperson_id,
        batteryid::number as battery_id,
        trim(accountingid)::varchar as accounting_id,
        engineerpersonid::number as engineerperson_id,
        groupid::number as group_id,
        platformid::number as platform_id,
        accountingteamid::number as accountingteam_id,
        trim(engineeringid)::varchar as engineering_id,
        haulerbeid::number as haulerbe_id,
        operatorentityid::number as operatorentity_id,
        purchaserbeid::number as purchaserbe_id,
        trim(productionid)::varchar as production_id,
        tankbatteryid::number as tankbattery_id,
        pumperpersonid::number as pumperperson_id,
        areaid::number as area_id,
        trim(purchasertankid)::varchar as purchasertank_id,
        drillingteamid::number as drillingteam_id,
        divisionid::number as division_id,
        gatheringsystemid::number as gathering_system_id,
        mastertankbatteryid::number as mastertankbattery_id,

        -- dates
        recorddate::timestamp_ntz as record_date,
        enddate::timestamp_ntz as end_date,
        datetimestamp::timestamp_ntz as date_timestamp,
        blogicdatestamp::timestamp_ntz as blogic_date_stamp,
        userdatestamp::timestamp_ntz as user_date_stamp,
        strappingdate::timestamp_ntz as strapping_date,
        allocationdatestamp::timestamp_ntz as allocation_date_stamp,
        startdate::timestamp_ntz as start_date,

        -- names and descriptions
        trim(allocationcomment)::varchar as allocationcomment,

        -- geography
        tankbatteryrole::number as tankbatteryrole,

        -- accounting/business entities
        totalhauleroil::float as totalhauleroil,
        totalhaulerwater::float as totalhaulerwater,
        waterpurchaser::number as waterpurchaser,
        haulerbeloc::number as haulerbeloc,
        totalrunshaulerentered::number as totalrunshaulerentered,
        waterhauler::number as waterhauler,
        purchaserbeloc::number as purchaserbeloc,
        operatorentityloc::number as operatorentityloc,
        trim(haulertanknumber)::varchar as haulertank_number,

        -- volumes
        productionngl::float as productionngl,
        nglproduction::float as nglproduction,
        oilproduction::float as oilproduction,
        productionallocdefault::number as productionallocdefault,
        waterproduction::float as waterproduction,

        -- allocation factors
        factorcomputationflag::number as factorcomputation_flag,
        gasequivalentfactor::float as gasequivalentfactor,
        tankvaporfactor::float as tankvaporfactor,

        -- operational/equipment
        totalrunspumperentered::number as totalrunspumperentered,

        -- flags
        calculationstatusflag::number as calculationstatus_flag,
        firstdayflag::number as firstday_flag,
        tankdataentryflag::number as tankdataentry_flag,
        backgroundtaskflag::number as backgroundtask_flag,
        activeflag::number as active_flag,
        allownegativeinventoryflag::number as allownegativeinventory_flag,
        loadtransferflag::number as loadtransfer_flag,

        -- audit/metadata
        trim(rowuid)::varchar as rowu_id,
        userid::number as user_id,
        trim(usertimestamp)::varchar as user_timestamp,
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_tz as _fivetran_synced,

        -- other
        tankmonthlyinvsource::number as tankmonthlyinvsource,
        endoil::float as endoil,
        bottomcloseinch::number as bottomcloseinch,
        allocationmethod::number as allocationmethod,
        bsandw::float as bsandw,
        topopeninch::number as topopeninch,
        topopenquarter::float as topopenquarter,
        allocationorder::number as allocationorder,
        topopenfeet::number as topopenfeet,
        dispositioncode::number as disposition_code,
        totalrunsngl::float as totalrunsngl,
        totalrunsoil::float as totalrunsoil,
        trim(allocationtimestamp)::varchar as allocation_timestamp,
        beginningoil::float as beginningoil,
        topclosequarter::float as topclosequarter,
        allocationbycomponent::number as allocationbycomponent,
        measurementpointrole::number as measurementpointrole,
        topclosetotalinches::float as topclosetotalinches,
        totaldispositionwater::float as totaldispositionwater,
        totaldispositionoil::float as totaldispositionoil,
        beginningngl::float as beginningngl,
        bottomopeninch::number as bottomopeninch,
        productcode::number as product_code,
        bottomopenfeet::number as bottomopenfeet,
        datasourcecode::number as datasource_code,
        tankvaporvolume::float as tankvaporvolume,
        tankcompbeginvsource::number as tankcompbeginvsource,
        bottomopenquarter::float as bottomopenquarter,
        topcloseinch::number as topcloseinch,
        totalrunswater::float as totalrunswater,
        bottomclosefeet::number as bottomclosefeet,
        endingngl::float as endingngl,
        topclosefeet::number as topclosefeet,
        dispositionallocdefault::number as dispositionallocdefault,
        producttype::number as product_type,
        bottomclosetotalinches::float as bottomclosetotalinches,
        bottomclosequarter::float as bottomclosequarter,
        tankdatasummarycode::number as tankdatasummary_code,
        bottomopentotalinches::float as bottomopentotalinches,
        topopentotalinches::float as topopentotalinches,
        endwater::float as endwater,
        trim(blogictimestamp)::varchar as blogic_timestamp,
        beginningwater::float as beginningwater

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
        {{ dbt_utils.generate_surrogate_key(['merrick_id', 'record_date']) }} as tankmonthly_sk,
        *,
        current_timestamp() as _loaded_at

    from filtered

),

final as (

    select
        tankmonthly_sk,

        -- identifiers
        merrick_id,
        fieldgroup_id,
        accountantperson_id,
        productionteam_id,
        superperson_id,
        foremanperson_id,
        battery_id,
        accounting_id,
        engineerperson_id,
        group_id,
        platform_id,
        accountingteam_id,
        engineering_id,
        haulerbe_id,
        operatorentity_id,
        purchaserbe_id,
        production_id,
        tankbattery_id,
        pumperperson_id,
        area_id,
        purchasertank_id,
        drillingteam_id,
        division_id,
        gathering_system_id,
        mastertankbattery_id,

        -- dates
        record_date,
        end_date,
        date_timestamp,
        blogic_date_stamp,
        user_date_stamp,
        strapping_date,
        allocation_date_stamp,
        start_date,

        -- names and descriptions
        allocationcomment,

        -- geography
        tankbatteryrole,

        -- accounting/business entities
        totalhauleroil,
        totalhaulerwater,
        waterpurchaser,
        haulerbeloc,
        totalrunshaulerentered,
        waterhauler,
        purchaserbeloc,
        operatorentityloc,
        haulertank_number,

        -- volumes
        productionngl,
        nglproduction,
        oilproduction,
        productionallocdefault,
        waterproduction,

        -- allocation factors
        factorcomputation_flag,
        gasequivalentfactor,
        tankvaporfactor,

        -- operational/equipment
        totalrunspumperentered,

        -- flags
        calculationstatus_flag,
        firstday_flag,
        tankdataentry_flag,
        backgroundtask_flag,
        active_flag,
        allownegativeinventory_flag,
        loadtransfer_flag,

        -- audit/metadata
        rowu_id,
        user_id,
        user_timestamp,
        _fivetran_deleted,
        _fivetran_synced,

        -- other
        tankmonthlyinvsource,
        endoil,
        bottomcloseinch,
        allocationmethod,
        bsandw,
        topopeninch,
        topopenquarter,
        allocationorder,
        topopenfeet,
        disposition_code,
        totalrunsngl,
        totalrunsoil,
        allocation_timestamp,
        beginningoil,
        topclosequarter,
        allocationbycomponent,
        measurementpointrole,
        topclosetotalinches,
        totaldispositionwater,
        totaldispositionoil,
        beginningngl,
        bottomopeninch,
        product_code,
        bottomopenfeet,
        datasource_code,
        tankvaporvolume,
        tankcompbeginvsource,
        bottomopenquarter,
        topcloseinch,
        totalrunswater,
        bottomclosefeet,
        endingngl,
        topclosefeet,
        dispositionallocdefault,
        product_type,
        bottomclosetotalinches,
        bottomclosequarter,
        tankdatasummary_code,
        bottomopentotalinches,
        topopentotalinches,
        endwater,
        blogic_timestamp,
        beginningwater,

        -- dbt metadata
        _loaded_at

    from enhanced

)

select * from final