{{
    config(
        materialized='view',
        tags=['griffin', 'intermediate', 'crescent']
    )
}}

/*
    Intermediate model enriching tank master data.
    Links tanks to parent completions for context.
*/

with

tanks as (

    select * from {{ ref('stg_procount__tanks') }}

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
        t.merrick_id as tank_merrick_id,
        t.location_merrick_id,
        t.battery_id,
        t.tankbattery_id,
        t.mastertankbattery_id,
        t.route_id as tank_route_id,
        t.gathering_system_id,
        t.lease_id as tank_lease_id,
        t.area_id as tank_area_id,
        t.division_id,
        t.facility_id,
        t.plant_id,
        t.pipeline_id,
        t.terminal_id,
        t.group_id,
        t.allocationgroup_id,
        
        -- tank identification
        t.tank_name,
        t.tankdescription,
        t.tank_type,
        t.tankconstruction,
        t.tankgauge_type,
        t.serial_number,
        t.make,
        t.model_number,
        t.manufacturerbe_id,
        
        -- external IDs
        t.scada_id,
        t.engineering_id,
        t.accounting_id,
        t.production_id,
        t.purchasertank_id,
        t.operatortank_number,
        t.haulertank_number,
        t.purchaserlocation_number,
        
        -- parent completion context (via location_merrick_id)
        c.well_name as parent_well_name,
        c.completion_name as parent_completion_name,
        c.wellpluscompletion_name as parent_wellpluscompletion_name,
        c.route_name as parent_route_name,
        c.property_number,

        -- business entities
        t.purchaserbe_id,
        t.purchaserbeloc,
        t.haulerbe_id,
        t.haulerbeloc,
        t.operatorentity_id,
        t.operatorentityloc,
        t.waterhauler,
        t.waterpurchaser,

        -- tank specs/capacity
        t.tanksize as capacity,
        t.barrelsperinch,
        t.barrelsperquarterinch,
        t.topfeet,
        t.topinches,
        t.topquarter,
        t.toptotalinches,

        -- strapping/measurement
        t.shellexpansioncoefficient,
        t.shellreferencetemperature,
        t.laststrapping_date,
        t.standardgaugetime,
        t.watercutaveragedays,

        -- product
        t.product_code,
        t.product_type,

        -- location
        t.latitude,
        t.longitude,
        t.statepointofdisposition,
        t.statepointofdispositionoil,
        t.statepointofdispositionwater,
        t.tankbatteryrole,

        -- regulatory
        t.mmsmeteringpoint,
        t.mmsfmp_number,
        t.measurementpointrole,

        -- status
        t.active_flag,
        t.delete_flag,

        -- dates
        t.start_date,
        t.end_date,
        t.startactive_date,
        t.endactive_date,
        t.recordcreation_date,

        -- allocation
        t.allocationtype_flag,
        t.allocationtypestart_date,
        t.allocationorder,
        t.allocauto_flag,
        t.allocautostart_date,
        t.fifoallocation_flag,

        -- calculation flags
        t.ignorebswvolume_flag,
        t.usevaporadjustment_flag,
        t.includeinventoryinregs_flag,
        t.insulated_flag,

        -- other flags
        t.transmit_flag,
        t.print_flag,
        t.carryforward_flag,
        t.sealrequired_flag,

        -- personnel
        t.pumperperson_id,
        t.foremanperson_id,
        t.engineerperson_id,
        t.superperson_id,
        t.accountantperson_id,

        -- teams
        t.productionteam_id,
        t.accountingteam_id,
        t.drillingteam_id,

        -- metadata
        t._fivetran_synced,
        t._loaded_at

    from tanks t
    left join completions c
        on t.location_merrick_id = c.merrick_id

),

final as (

    select * from enriched

)

select * from final
