{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for IFS Procount meters.
    
    ONE-TO-ONE with source table: meter_tb_barnett_sheet_1
    
    No joins - just rename and light casting.
*/

with source as (
    select * from {{ source('seeds_raw', 'meter_tb_barnett_sheet_1') }}
),

renamed as (
    select
        -- Primary Key
        _line as procount_line_number,
        merrickid as meter_id,
        
        -- Business Identifiers
        metername as meter_name,
        meternumber as meter_number,
        meterdescription as meter_description,
        purchasermeterid as purchaser_meter_id,
        
        -- Meter Configuration
        metertype as meter_type,
        measurementpointrole as measurement_point_role,
        metercalculationflag as meter_calculation_flag,
        metertestfrequency as meter_test_frequency,
        odometermaximum as odometer_maximum,
        absolutepressure as absolute_pressure,
        
        -- Foreign Keys
        batteryid as battery_id,
        gatheringsystemid as gathering_system_id,
        facilityid as facility_id,
        platformid as platform_id,
        routeid as route_id,
        terminalid as terminal_id,
        pipelineid as pipeline_id,
        checkmetermerrickid as check_meter_merrick_id,
        
        -- Allocation
        allocationtypeflag as allocation_type_flag,
        allocationgroupid as allocation_group_id,
        allocationorder as allocation_order,
        allocationbycomponent as allocation_by_component,
        allocautoflag as alloc_auto_flag,
        allocautostartdate as alloc_auto_start_date,
        allocationruntwicemonthly as allocation_run_twice_monthly,
        allocationruntwicedaily as allocation_run_twice_daily,
        
        -- Product
        producttype as product_type,
        productcode as product_code,
        
        -- Business Entities
        purchaserbeid as purchaser_be_id,
        purchaserbeloc as purchaser_be_loc,
        transporterbeid as transporter_be_id,
        transporterbeloc as transporter_be_loc,
        gathererbeid as gatherer_be_id,
        gathererbeloc as gatherer_be_loc,
        integratorbeid as integrator_be_id,
        integratorbeloc as integrator_be_loc,
        meteroperatorbeid as meter_operator_be_id,
        meteroperatorbeloc as meter_operator_be_loc,
        manufacturerbeid as manufacturer_be_id,
        
        -- Dates
        startdate as start_date,
        enddate as end_date,
        startactivedate as start_active_date,
        endactivedate as end_active_date,
        standardreadingtime as standard_reading_time,
        
        -- Geographic
        stateid as state_id,
        countyid as county_id,
        areaid as area_id,
        divisionid as division_id,
        fieldgroupid as field_group_id,
        leaseid as lease_id,
        groupid as group_id,
        plantid as plant_id,
        regulatoryfieldid as regulatory_field_id,
        
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
        
        -- Flags
        activeflag as active_flag,
        deleteflag as delete_flag,
        outsideoperatedflag as outside_operated_flag,
        prorationcalcflag as proration_calc_flag,
        monthlydatasourceflag as monthly_data_source_flag,
        ignorebswvolumeflag as ignore_bsw_volume_flag,
        allocateusingfixedvolflag as allocate_using_fixed_vol_flag,
        copyrunticketvolumeflag as copy_run_ticket_volume_flag,
        copyconvertvolumeflag as copy_convert_volume_flag,
        btucopyflag as btu_copy_flag,
        fuelfactorflag as fuel_factor_flag,
        statefilingflag as state_filing_flag,
        computefactorflag as compute_factor_flag,
        
        -- Configuration
        unitstypeflag as units_type_flag,
        locationorder as location_order,
        locationmerrickid as location_merrick_id,
        locationid as location_id,
        meterchildcount as meter_child_count,
        
        -- Gas Analysis
        gasanalysissourceid as gas_analysis_source_id,
        gasanalysissourcetype as gas_analysis_source_type,
        
        -- Metadata
        userid as user_id,
        userdatestamp as user_date_stamp,
        usertimestamp as user_time_stamp,
        datetimestamp as date_time_stamp,
        blogicdatestamp as blogic_date_stamp,
        blogictimestamp as blogic_time_stamp,
        rowuid as row_uid,
        scadaid as scada_id,
        
        -- Fivetran
        _fivetran_synced,
        _fivetran_deleted

    from source
)

select * from renamed