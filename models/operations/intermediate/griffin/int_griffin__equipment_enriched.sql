{{
    config(
        materialized='view',
        tags=['griffin', 'intermediate', 'crescent']
    )
}}

/*
    Intermediate model enriching equipment master data.
    Links equipment to parent completions for context.
*/

with

equipment as (

    select * from {{ ref('stg_procount__equipment') }}

),

completions as (

    select
        merrick_id,
        well_name,
        completion_name,
        wellpluscompletion_name,
        route_id,
        route_name,
        property_number,
        lease_id,
        area_id
    from {{ ref('int_griffin__completions_enriched') }}

),

enriched as (

    select
        -- identifiers
        e.merrick_id as equipment_merrick_id,
        e.location_merrick_id,
        e.battery_id,
        e.route_id as equipment_route_id,
        e.gathering_system_id,
        e.lease_id as equipment_lease_id,
        e.area_id as equipment_area_id,
        e.division_id,
        e.facility_id,
        e.plant_id,
        e.group_id,
        e.allocationgroup_id,
        
        -- equipment identification
        e.equipment_name,
        e.equipment_number,
        e.equipmentdescription,
        e.equipment_type,
        e.serial_number,
        e.sku_number,
        e.make,
        e.model_number,
        e.manufacturerbe_id,
        
        -- parent completion context (via location_merrick_id)
        c.well_name as parent_well_name,
        c.completion_name as parent_completion_name,
        c.wellpluscompletion_name as parent_wellpluscompletion_name,
        c.route_name as parent_route_name,
        c.property_number,

        -- equipment specs
        e.horsepower,
        e.numberofstages,

        -- rental info
        e.equipmentowner_id,
        e.equipmentownerloc,
        e.rent_date,
        e.baserentalcharges,
        e.maintenancecharges,
        e.insurancecharges,
        e.pumpercharges,
        e.rentaltermsmonths,
        e.purchaseoption,
        e.agreement_number,
        e.rentalcompanyunit_number,
        e.internalunit_number,

        -- status
        e.active_flag,
        e.delete_flag,

        -- dates
        e.dateinstalled as install_date,
        e.startactive_date,
        e.endactive_date,
        e.recordcreation_date,

        -- allocation
        e.allocationtype_flag,
        e.allocationtypestart_date,
        e.allocationorder,
        e.allocauto_flag,
        e.allocautostart_date,

        -- product
        e.product_code,
        e.product_type,
        e.disposition_code,

        -- flags
        e.transmit_flag,
        e.print_flag,
        e.carryforward_flag,
        e.templaterecord_flag,

        -- personnel
        e.pumperperson_id,
        e.foremanperson_id,
        e.engineerperson_id,
        e.superperson_id,
        e.accountantperson_id,

        -- teams
        e.productionteam_id,
        e.accountingteam_id,
        e.drillingteam_id,

        -- metadata
        e._fivetran_synced,
        e._loaded_at

    from equipment e
    left join completions c
        on e.location_merrick_id = c.merrick_id

),

final as (

    select * from enriched

)

select * from final
