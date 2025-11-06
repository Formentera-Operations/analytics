{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for IFS Procount completions.
    
    ONE-TO-ONE with source table: completion_tb_barnett_sheet_1
    
    No joins - just rename and light casting.
*/

with source as (
    select * from {{ source('seeds_raw', 'completion_tb_barnett_sheet_1') }}
),

renamed as (
    select
        -- Primary Key
    
        merrickid as completion_id,
        
        -- Business Identifiers
        wellpluscompletionname as well_completion_name,
        wellname as well_name,
        uwi,
        uwidisplay as uwi_display,
        apiwellnumber as api_well_number,
        propertynumber as property_number,
        statewellnumber as state_well_number,
        stateapiwellnumber as state_api_well_number,
        stateleasename as state_lease_name,
        
        -- Foreign Keys
        propertywellid as property_well_id,
        batteryid as battery_id,
        gatheringsystemid as gathering_system_id,
        injectiongatheringsystemid as injection_gathering_system_id,
        facilityid as facility_id,
        platformid as platform_id,
        unitid as unit_id,
        reservoirid as reservoir_id,
        formationid as formation_id,
        routeid as route_id,
        
        -- Status & Method (these are the ACTUAL column names per DDL)
        producingstatus as producing_status,
        producingmethod as producing_method,
        lastproducingstatus as last_producing_status,
        lastproducingmethod as last_producing_method,
        completionstatus as completion_status,
        
        -- Allocation
        allocationtypeflag as allocation_type_flag,
        allocationgroupid as allocation_group_id,
        allocautoflag as alloc_auto_flag,
        allocautostartdate as alloc_auto_start_date,
        
        -- Dates (already correct types per DDL)
        datecompleted as date_completed,
        dateshutin as date_shut_in,
        dateabandoned as date_abandoned,
        datewellacquired as date_well_acquired,
        datewellsold as date_well_sold,
        dateofinitialproductionoil as date_initial_production_oil,
        dateofinitialproductiongas as date_initial_production_gas,
        rigreleasedate as rig_release_date,
        firstoilsales as first_oil_sales,
        firstgassales as first_gas_sales,
        lastproductiondate as last_production_date,
        lasttestdate as last_test_date,
        laststatuschangeddate as last_status_change_date,
        cumulativestartmonth as cumulative_start_month,
        
        -- Test Results
        lasttestoil as last_test_oil,
        lasttestgas as last_test_gas,
        lasttestwater as last_test_water,
        
        -- Depths
        fromdepth as from_depth,
        todepth as to_depth,
        secondfromdepth as second_from_depth,
        secondtodepth as second_to_depth,
        thirdfromdepth as third_from_depth,
        thirdtodepth as third_to_depth,
        packerdepth as packer_depth,
        kellybushinglevel as kelly_bushing_level,
        
        -- Completion Details
        completiontype as completion_type,
        completionflowdirection as completion_flow_direction,
        completionfluidtype as completion_fluid_type,
        
        -- Reserves
        ultimatereservesoil as ultimate_reserves_oil,
        ultimatereservesgas as ultimate_reserves_gas,
        ultimatereserveswater as ultimate_reserves_water,
        reservecatagory as reserve_category,
        
        -- Geographic
        stateid as state_id,
        countyid as county_id,
        countryid as country_id,
        areaid as area_id,
        divisionid as division_id,
        fieldgroupid as field_group_id,
        leaseid as lease_id,
        groupid as group_id,
        
        -- Coordinates
        latitude,
        longitude,
        mapcoordinatex as map_coordinate_x,
        mapcoordinatey as map_coordinate_y,
        
        -- Operator
        operatorentityid as operator_entity_id,
        operatorentityloc as operator_entity_loc,
        
        -- Personnel
        pumperpersonid as pumper_person_id,
        foremanpersonid as foreman_person_id,
        superpersonid as super_person_id,
        accountantpersonid as accountant_person_id,
        engineerpersonid as engineer_person_id,
        
        -- Teams
        productionteamid as production_team_id,
        drillingteamid as drilling_team_id,
        accountingteamid as accounting_team_id,
        
        -- Flags (keep as NUMBER)
        activeflag as active_flag,
        deleteflag as delete_flag,
        productionactiveflag as production_active_flag,
        outsideoperatedflag as outside_operated_flag,
        offshoreflag as offshore_flag,
        directionalwellflag as directional_well_flag,
        marginalwellflag as marginal_well_flag,
        stripperqualifiedwellflag as stripper_qualified_well_flag,
        splitstreamflag as split_stream_flag,
        
        -- Regulatory
        statefilingflag as state_filing_flag,
        statefilingid as state_filing_id,
        regulatoryfieldid as regulatory_field_id,
        regsreservoirid as regs_reservoir_id,
        
        -- Configuration
        unitstypeflag as units_type_flag,
        allocationruntwicemonthly as allocation_run_twice_monthly,
        allocationruntwicedaily as allocation_run_twice_daily,
        
        -- Metadata
        userid as user_id,
        userdatestamp as user_date_stamp,
        usertimestamp as user_time_stamp,
        datetimestamp as date_time_stamp,
        blogicdatestamp as blogic_date_stamp,
        blogictimestamp as blogic_time_stamp,
        rowuid as row_uid

    from source
)

select * from renamed