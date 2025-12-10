{{
    config(
        materialized='view',
        tags=['procount', 'staging', 'crescent']
    )
}}

with

source as (

    select * from {{ source('procount', 'COMPLETIONWELLTESTTB') }}

),

renamed as (

    select
        -- identifiers
        merrickid::number as merrick_id,
        commentserviceid::number as commentservice_id,
        gasfluidratio::float as gasfluidratio,
        chlorideppm::float as chlorideppm,
        gasliquidsgpm::float as gasliquidsgpm,
        injectionfluidtype::number as injectionfluid_type,
        fluidlevel::float as fluidlevel,
        shutinfluidlevel::float as shutinfluidlevel,
        trim(dynacapid)::varchar as dynacap_id,
        validtestflag::number as validtest_flag,
        formid::number as form_id,
        testtypeid::number as testtype_id,
        powerfluidrate::float as powerfluidrate,
        separatormeterid::number as separatormeter_id,
        powerfluidpressure::float as powerfluidpressure,

        -- dates
        recorddate::timestamp_ntz as record_date,
        trim(starttesttime)::varchar as starttesttime,
        endtestdate::timestamp_ntz as endtest_date,
        allocationdatestamp::timestamp_ntz as allocation_date_stamp,
        starttestdate::timestamp_ntz as starttest_date,
        blogicdatestamp::timestamp_ntz as blogic_date_stamp,
        potentialtestdate::timestamp_ntz as potentialtest_date,
        regulatoryeffectivedate::timestamp_ntz as regulatoryeffective_date,
        trim(recordtime)::varchar as recordtime,
        datetimestamp::timestamp_ntz as date_timestamp,
        trim(lastloadtime)::varchar as lastloadtime,
        allocationeffectivedate::timestamp_ntz as allocationeffective_date,
        endflowdate::timestamp_ntz as endflow_date,
        trim(completedtime)::varchar as completedtime,
        startflowdate::timestamp_ntz as startflow_date,
        duedate::timestamp_ntz as due_date,
        trim(endflowtime)::varchar as endflowtime,
        lastloaddate::timestamp_ntz as lastload_date,
        trim(startflowtime)::varchar as startflowtime,
        completeddate::timestamp_ntz as completed_date,
        userdatestamp::timestamp_ntz as user_date_stamp,
        runtimeclockpercentage::float as runtimeclockpercentage,
        trim(endtesttime)::varchar as endtesttime,

        -- well/completion attributes
        welltestreasoncode::number as welltestreason_code,
        gaugedepth::float as gaugedepth,

        -- names and descriptions
        trim(reasoncomment)::varchar as reasoncomment,
        trim(testcomment)::varchar as testcomment,

        -- geography
        keyesfieldannpresconst::float as keyesfieldannpresconst,
        keyesfieldflowcoef::float as keyesfieldflowcoef,

        -- volumes
        injectionproducttype::number as injectionproduct_type,
        co2production::float as co2production,
        oilproductionnet::float as oilproductionnet,
        dailyproductionrateoil::float as dailyproductionrateoil,
        testgasvolume::float as testgasvolume,
        dailyproductionratecond::float as dailyproductionratecond,
        dailyproductionrategas::float as dailyproductionrategas,
        injectionpressure::float as injectionpressure,
        testwatervolume::float as testwatervolume,
        injectionvolume::float as injectionvolume,
        waterproduction::float as waterproduction,
        injectionproductcode::number as injectionproduct_code,
        oilproduction::float as oilproduction,
        gasinjection::float as gasinjection,
        gasproductionnet::float as gasproductionnet,
        injectionliftflag::number as injectionlift_flag,
        dailyproductionratewater::float as dailyproductionratewater,
        co2injection::float as co2injection,
        testoilvolume::float as testoilvolume,
        gasproduction::float as gasproduction,

        -- rates
        dailyinterpolationratewater::float as dailyinterpolationratewater,
        dailyinterpolationrategas::float as dailyinterpolationrategas,
        dailyinterpolationrateoil::float as dailyinterpolationrateoil,
        gasliftinputgasrate::float as gasliftinputgasrate,
        maximumefficientrate::float as maximumefficientrate,
        gasrate24hourflag::number as gasrate24hour_flag,

        -- pressures
        flowingwellheadpressure::float as flowingwellheadpressure,
        separatorpressure::float as separatorpressure,
        barometricpressure::float as barometricpressure,
        shutinwellheadpressure::float as shutinwellheadpressure,
        staticpressure::float as staticpressure,
        pumpsize::float as pumpsize,
        meterpressure::float as meterpressure,
        tubingpressure::float as tubingpressure,
        averagepoolpressure::float as averagepoolpressure,
        flowingtubingpressure::float as flowingtubingpressure,
        shutincasingpressure::float as shutincasingpressure,
        calculatedpressure::float as calculatedpressure,
        linepressure::float as linepressure,
        shutintubingpressure::float as shutintubingpressure,
        differentialpressure::float as differentialpressure,
        criticalpressure::float as criticalpressure,
        workingpressure::float as workingpressure,
        surfacecasingpressure::float as surfacecasingpressure,
        shutinpressure::float as shutinpressure,
        staticcolumnwhpressure::float as staticcolumnwhpressure,
        flowingcasingpressure::float as flowingcasingpressure,
        shutinbottomholepressure::float as shutinbottomholepressure,
        bottomholepressure::float as bottomholepressure,
        pressureclassification::number as pressureclassification,
        pressurebase::float as pressurebase,

        -- temperatures
        criticaltemperature::float as criticaltemperature,
        bottomholetemp::float as bottomholetemp,
        shutintubingtemperature::float as shutintubingtemperature,
        flowingtemperature::float as flowingtemperature,
        temperaturedepth::float as temperaturedepth,
        surfacetemperature::float as surfacetemperature,
        shutinbottomholetemp::float as shutinbottomholetemp,
        shutincasingtemperature::float as shutincasingtemperature,
        observedtemperature::float as observedtemperature,
        ambientairtemperature::float as ambientairtemperature,

        -- allocation factors
        specificgravity::float as specificgravity,
        gasgravity::float as gasgravity,
        supercompressfactor::float as supercompressfactor,
        btufactor::float as btufactor,
        gravityoil::float as gravityoil,
        keyesfieldwellpresfactor::float as keyesfieldwellpresfactor,
        apigravity::float as apigravity,
        rotationalpumpfactor::float as rotationalpumpfactor,
        correctionfactor::float as correctionfactor,
        flowcoefficient::float as flowcoefficient,
        gravitywater::float as gravitywater,

        -- operational/equipment
        pumphours::float as pumphours,
        strokesperminute::float as strokesperminute,
        chokesize::float as chokesize,
        pretesthours::float as pretesthours,
        pumpfrequency::float as pumpfrequency,
        pumprpm::float as pumprpm,
        testhours::float as testhours,
        flowdurationhours::float as flowdurationhours,
        pumptype::number as pump_type,
        strokeline::float as strokeline,
        testdurationhours::float as testdurationhours,
        shutindurationhours::float as shutindurationhours,

        -- flags
        wetdryflag::number as wetdry_flag,
        calculationstatusflag::number as calculationstatus_flag,
        backgroundtaskflag::number as backgroundtask_flag,
        measuredtestflag::number as measuredtest_flag,
        runefficencycalcflag::number as runefficencycalc_flag,
        usedinallocationflag::number as usedinallocation_flag,
        deleteflag::number as delete_flag,
        regulatorytestflag::number as regulatorytest_flag,
        unitsflag::number as units_flag,
        testallocationflag::number as testallocation_flag,
        detailcalcflag::number as detailcalc_flag,
        transmitflag::number as transmit_flag,
        meterrunflag::number as meterrun_flag,

        -- audit/metadata
        trim(rowuid)::varchar as rowu_id,
        trim(usertimestamp)::varchar as user_timestamp,
        userid::number as user_id,
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_tz as _fivetran_synced,

        -- other
        numberofphases::number as numberofphases,
        sourcemetergasinj::number as sourcemetergasinj,
        meterspringsize::float as meterspringsize,
        calculatedaof::float as calculatedaof,
        sourcemetergas::number as sourcemetergas,
        dailycoefficent::float as dailycoefficent,
        efficiency::float as efficiency,
        slope::float as slope,
        mixingtuberatio::float as mixingtuberatio,
        flowchannellength::float as flowchannellength,
        gaugedifferential::float as gaugedifferential,
        connectiontype::float as connection_type,
        meterproversize::float as meterproversize,
        sourcemeterenabled::number as sourcemeterenabled,
        numberofgasstreams::number as numberofgasstreams,
        separatorpresure::float as separatorpresure,
        gastestmethodcode::number as gastestmethod_code,
        sourcemeterwater::number as sourcemeterwater,
        gasoilratio::float as gasoilratio,
        testedby::number as testedby,
        lasttransmission::number as lasttransmission,
        productivityindex::float as productivityindex,
        ironppm::float as ironppm,
        flowsize::float as flowsize,
        orificesize::float as orificesize,
        regulatoryagency::number as regulatoryagency,
        meterrunsize::float as meterrunsize,
        bswpercent::float as bswpercent,
        sourcemeteroil::number as sourcemeteroil,
        trim(circulationdirection)::varchar as circulationdirection,
        metertype::number as meter_type,
        trim(blogictimestamp)::varchar as blogic_timestamp,
        filingcode::number as filing_code,
        sourcemeterwaterinj::number as sourcemeterwaterinj,
        gasdeliverability::float as gasdeliverability,
        watercut::float as watercut,
        datasourcecode::number as datasource_code,
        viscosity::float as viscosity,
        sourcemeteroilinj::number as sourcemeteroilinj,
        gasconcentrationamount::float as gasconcentrationamount,
        sandpercent::float as sandpercent

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
        {{ dbt_utils.generate_surrogate_key(['merrick_id', 'record_date']) }} as completionwelltest_sk,
        *,
        current_timestamp() as _loaded_at

    from filtered

),

final as (

    select
        completionwelltest_sk,

        -- identifiers
        merrick_id,
        commentservice_id,
        gasfluidratio,
        chlorideppm,
        gasliquidsgpm,
        injectionfluid_type,
        fluidlevel,
        shutinfluidlevel,
        dynacap_id,
        validtest_flag,
        form_id,
        testtype_id,
        powerfluidrate,
        separatormeter_id,
        powerfluidpressure,

        -- dates
        record_date,
        starttesttime,
        endtest_date,
        allocation_date_stamp,
        starttest_date,
        blogic_date_stamp,
        potentialtest_date,
        regulatoryeffective_date,
        recordtime,
        date_timestamp,
        lastloadtime,
        allocationeffective_date,
        endflow_date,
        completedtime,
        startflow_date,
        due_date,
        endflowtime,
        lastload_date,
        startflowtime,
        completed_date,
        user_date_stamp,
        runtimeclockpercentage,
        endtesttime,

        -- well/completion attributes
        welltestreason_code,
        gaugedepth,

        -- names and descriptions
        reasoncomment,
        testcomment,

        -- geography
        keyesfieldannpresconst,
        keyesfieldflowcoef,

        -- volumes
        injectionproduct_type,
        co2production,
        oilproductionnet,
        dailyproductionrateoil,
        testgasvolume,
        dailyproductionratecond,
        dailyproductionrategas,
        injectionpressure,
        testwatervolume,
        injectionvolume,
        waterproduction,
        injectionproduct_code,
        oilproduction,
        gasinjection,
        gasproductionnet,
        injectionlift_flag,
        dailyproductionratewater,
        co2injection,
        testoilvolume,
        gasproduction,

        -- rates
        dailyinterpolationratewater,
        dailyinterpolationrategas,
        dailyinterpolationrateoil,
        gasliftinputgasrate,
        maximumefficientrate,
        gasrate24hour_flag,

        -- pressures
        flowingwellheadpressure,
        separatorpressure,
        barometricpressure,
        shutinwellheadpressure,
        staticpressure,
        pumpsize,
        meterpressure,
        tubingpressure,
        averagepoolpressure,
        flowingtubingpressure,
        shutincasingpressure,
        calculatedpressure,
        linepressure,
        shutintubingpressure,
        differentialpressure,
        criticalpressure,
        workingpressure,
        surfacecasingpressure,
        shutinpressure,
        staticcolumnwhpressure,
        flowingcasingpressure,
        shutinbottomholepressure,
        bottomholepressure,
        pressureclassification,
        pressurebase,

        -- temperatures
        criticaltemperature,
        bottomholetemp,
        shutintubingtemperature,
        flowingtemperature,
        temperaturedepth,
        surfacetemperature,
        shutinbottomholetemp,
        shutincasingtemperature,
        observedtemperature,
        ambientairtemperature,

        -- allocation factors
        specificgravity,
        gasgravity,
        supercompressfactor,
        btufactor,
        gravityoil,
        keyesfieldwellpresfactor,
        apigravity,
        rotationalpumpfactor,
        correctionfactor,
        flowcoefficient,
        gravitywater,

        -- operational/equipment
        pumphours,
        strokesperminute,
        chokesize,
        pretesthours,
        pumpfrequency,
        pumprpm,
        testhours,
        flowdurationhours,
        pump_type,
        strokeline,
        testdurationhours,
        shutindurationhours,

        -- flags
        wetdry_flag,
        calculationstatus_flag,
        backgroundtask_flag,
        measuredtest_flag,
        runefficencycalc_flag,
        usedinallocation_flag,
        delete_flag,
        regulatorytest_flag,
        units_flag,
        testallocation_flag,
        detailcalc_flag,
        transmit_flag,
        meterrun_flag,

        -- audit/metadata
        rowu_id,
        user_timestamp,
        user_id,
        _fivetran_deleted,
        _fivetran_synced,

        -- other
        numberofphases,
        sourcemetergasinj,
        meterspringsize,
        calculatedaof,
        sourcemetergas,
        dailycoefficent,
        efficiency,
        slope,
        mixingtuberatio,
        flowchannellength,
        gaugedifferential,
        connection_type,
        meterproversize,
        sourcemeterenabled,
        numberofgasstreams,
        separatorpresure,
        gastestmethod_code,
        sourcemeterwater,
        gasoilratio,
        testedby,
        lasttransmission,
        productivityindex,
        ironppm,
        flowsize,
        orificesize,
        regulatoryagency,
        meterrunsize,
        bswpercent,
        sourcemeteroil,
        circulationdirection,
        meter_type,
        blogic_timestamp,
        filing_code,
        sourcemeterwaterinj,
        gasdeliverability,
        watercut,
        datasource_code,
        viscosity,
        sourcemeteroilinj,
        gasconcentrationamount,
        sandpercent,

        -- dbt metadata
        _loaded_at

    from enhanced

)

select * from final