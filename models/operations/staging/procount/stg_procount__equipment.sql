{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for IFS Procount equipment.
    
    ONE-TO-ONE with source table: equipment_tb_barnett_sheet_1
    
    No joins - just rename and light casting.
*/

with source as (
    select * from {{ source('seeds_raw', 'equipment_tb_barnett_sheet_1') }}
),

renamed as (
    select
        -- Primary Key
        _line as procount_line_number,
        merrickid as equipment_id,
        
        -- Business Identifiers
        equipmentname as equipment_name,
        equipmentnumber as equipment_number,
        equipmentdescription as equipment_description,
        
        -- Equipment Details
        equipmenttype as equipment_type,
        make,
        modelnumber as model_number,
        serialnumber as serial_number,
        horsepower,
        numberofstages as number_of_stages,
        
        -- Foreign Keys
        batteryid as battery_id,
        gatheringsystemid as gathering_system_id,
        facilityid as facility_id,
        platformid as platform_id,
        routeid as route_id,
        
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
        measurementpointrole as measurement_point_role,
        dispositioncode as disposition_code,
        
        -- Lease Use
        leaseusecoefficienttype as lease_use_coefficient_type,
        leaseusecoefficient as lease_use_coefficient,
        
        -- Owner/Manufacturer
        equipmentownerid as equipment_owner_id,
        equipmentownerloc as equipment_owner_loc,
        manufacturerbeid as manufacturer_be_id,
        
        -- Rental Information
        rentdate as rent_date,
        dateinstalled as date_installed,
        rentaltermsmonths as rental_terms_months,
        purchaseoption as purchase_option,
        agreementnumber as agreement_number,
        baserentalcharges as base_rental_charges,
        pumpercharges as pumper_charges,
        maintenancecharges as maintenance_charges,
        insurancecharges as insurance_charges,
        
        -- Dates
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
        
        -- Flags
        activeflag as active_flag,
        deleteflag as delete_flag,
        outsideoperatedflag as outside_operated_flag,
        volumeautopopulateflag as volume_auto_populate_flag,
        
        -- Configuration
        unitstypeflag as units_type_flag,
        locationorder as location_order,
        locationmerrickid as location_merrick_id,
        completionchildcount as completion_child_count,
        meterchildcount as meter_child_count,
        completionmethod as completion_method,
        
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
        
        -- Fivetran
        _fivetran_synced,
        _fivetran_deleted

    from source
)

select * from renamed