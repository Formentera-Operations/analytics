{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for IFS Procount tanks.
    
    ONE-TO-ONE with source table: tank_tb_barnett_sheet_1
    
    No joins - just rename and light casting.
*/

with source as (
    select * from {{ source('seeds_raw', 'tank_tb_barnett_sheet_1') }}
),

renamed as (
    select
        -- Primary Key
        _line as procount_line_number,
        merrickid as tank_id,
        
        -- Business Identifiers
        tankname as tank_name,
        tankdescription as tank_description,
        accountingid as accounting_id,
        
        -- Tank Configuration
        tanktype as tank_type,
        tankbatteryrole as tank_battery_role,
        tanksize as tank_size,
        tankgaugetype as tank_gauge_type,
        tankconstruction as tank_construction,
        
        -- Capacity & Strapping
        barrelsperinch as barrels_per_inch,
        barrelsperquarterinch as barrels_per_quarter_inch,
        topfeet as top_feet,
        topinches as top_inches,
        topquarter as top_quarter,
        toptotalinches as top_total_inches,
        laststrappingdate as last_strapping_date,
        
        -- Foreign Keys
        batteryid as battery_id,
        tankbatteryid as tank_battery_id,
        mastertankbatteryid as master_tank_battery_id,
        gatheringsystemid as gathering_system_id,
        facilityid as facility_id,
        platformid as platform_id,
        routeid as route_id,
        terminalid as terminal_id,
        pipelineid as pipeline_id,
        
        -- Allocation
        allocationtypeflag as allocation_type_flag,
        allocationgroupid as allocation_group_id,
        allocationorder as allocation_order,
        allocautoflag as alloc_auto_flag,
        allocautostartdate as alloc_auto_start_date,
        allocationruntwicemonthly as allocation_run_twice_monthly,
        allocationruntwicedaily as allocation_run_twice_daily,
        
        -- Product
        producttype as product_type,
        productcode as product_code,
        measurementpointrole as measurement_point_role,
        
        -- Purchaser/Hauler
        purchaserbeid as purchaser_be_id,
        purchaserbeloc as purchaser_be_loc,
        purchaserlocationnumber as purchaser_location_number,
        purchasertankid as purchaser_tank_id,
        haulerbeid as hauler_be_id,
        haulerbeloc as hauler_be_loc,
        haulertanknumber as hauler_tank_number,
        waterpurchaser as water_purchaser,
        waterhauler as water_hauler,
        
        -- Operator
        operatorentityid as operator_entity_id,
        operatorentityloc as operator_entity_loc,
        operatortanknumber as operator_tank_number,
        
        -- Dates
        startdate as start_date,
        enddate as end_date,
        startactivedate as start_active_date,
        endactivedate as end_active_date,
        
        -- Geographic
        stateid as state_id,
        countyid as county_id,
        areaid as area_id,
        divisionid as division_id,
        fieldgroupid as field_group_id,
        leaseid as lease_id,
        groupid as group_id,
        plantid as plant_id,
        
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
        
        -- Tank Settings
        standardgaugetime as standard_gauge_time,
        watercutaveragedays as water_cut_average_days,
        shellreferencetemperature as shell_reference_temperature,
        shellexpansioncoefficient as shell_expansion_coefficient,
        
        -- Flags
        activeflag as active_flag,
        deleteflag as delete_flag,
        outsideoperatedflag as outside_operated_flag,
        insulatedflag as insulated_flag,
        ignorebswvolumeflag as ignore_bsw_volume_flag,
        usevaporadjustmentflag as use_vapor_adjustment_flag,
        includeinventoryinregsflag as include_inventory_in_regs_flag,
        fifoallocationflag as fifo_allocation_flag,
        
        -- Configuration
        unitstypeflag as units_type_flag,
        locationorder as location_order,
        locationmerrickid as location_merrick_id,
        completionchildcount as completion_child_count,
        
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