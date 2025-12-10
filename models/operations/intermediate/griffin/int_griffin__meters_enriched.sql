{{
    config(
        materialized='view',
        tags=['griffin', 'intermediate', 'crescent']
    )
}}

/*
    Intermediate model enriching meter master data.
    Links meters to parent completions for context.
*/

with

meters as (

    select * from {{ ref('stg_procount__meters') }}

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
        area_id,
        gathering_system_id,
        gathering_system_name
    from {{ ref('int_griffin__completions_enriched') }}

),

enriched as (

    select
        -- identifiers
        m.merrick_id as meter_merrick_id,
        m.location_merrick_id,
        m.battery_id,
        m.route_id as meter_route_id,
        m.gathering_system_id as meter_gathering_system_id,
        m.lease_id as meter_lease_id,
        m.area_id as meter_area_id,
        m.division_id,
        m.facility_id,
        m.plant_id,
        m.pipeline_id,
        m.terminal_id,
        m.group_id,
        m.allocationgroup_id,
        m.checkmetermerrick_id,
        
        -- meter identification
        m.meter_name,
        m.meter_number,
        m.meterdescription,
        m.meter_type,
        m.serial_number,
        m.sku_number,
        m.make,
        m.model_number,
        m.manufacturerbe_id,
        
        -- external IDs
        m.scada_id,
        m.engineering_id,
        m.accounting_id,
        m.production_id,
        m.operatormeter_id,
        m.purchasermeter_id,
        m.integratormeter_id,
        m.standardmeter_id,
        m.accountingtransportmeter_id,
        
        -- parent completion context (via location_merrick_id)
        c.well_name as parent_well_name,
        c.completion_name as parent_completion_name,
        c.wellpluscompletion_name as parent_wellpluscompletion_name,
        c.route_name as parent_route_name,
        c.property_number,
        c.gathering_system_name,

        -- business entities
        m.purchaserbe_id,
        m.purchaserbeloc,
        m.gathererbe_id,
        m.gathererbeloc,
        m.gatherer_id,
        m.transporterbe_id,
        m.transporterbeloc,
        m.transporter_id,
        m.integratorbe_id,
        m.integratorbeloc,
        m.meteroperatorbe_id,
        m.meteroperatorbeloc,

        -- product
        m.product_code,
        m.product_type,
        m.disposition_code,

        -- meter specs
        m.absolutepressure,
        m.odometermaximum,
        m.metertestfrequency,

        -- location
        m.latitude,
        m.longitude,
        m.statepointofdisposition,
        m.regionarea,
        m.legaldescription,
        m.township,
        m.range,
        m.section,
        m.block,
        m.survey,

        -- regulatory
        m.regulatoryfacility_id,
        m.regulatoryfacilitylong_id,
        m.regulatoryfacility_type,
        m.regulatoryfield_id,
        m.statefiling_flag,
        m.stateplant_number,
        m.mmsgasplant_number,
        m.mmsfmp_number,

        -- status
        m.active_flag,
        m.delete_flag,

        -- dates
        m.start_date,
        m.end_date,
        m.startactive_date,
        m.endactive_date,
        m.recordcreation_date,

        -- allocation
        m.allocationtype_flag,
        m.allocationtypestart_date,
        m.allocationorder,
        m.allocauto_flag,
        m.allocautostart_date,
        m.allocationbycomponent,
        m.measurementpointrole,

        -- calculation flags
        m.metercalculation_flag,
        m.computefactor_flag,
        m.fuelfactor_flag,
        m.prorationcalc_flag,
        m.copyrunticketvolume_flag,
        m.copyconvertvolume_flag,
        m.allocateusingfixedvol_flag,
        m.ignorebswvolume_flag,
        m.btucopy_flag,

        -- other flags
        m.transmit_flag,
        m.print_flag,
        m.carryforward_flag,
        m.templaterecord_flag,
        m.monthlydatasource_flag,

        -- personnel
        m.pumperperson_id,
        m.foremanperson_id,
        m.engineerperson_id,
        m.superperson_id,
        m.accountantperson_id,

        -- teams
        m.productionteam_id,
        m.accountingteam_id,
        m.drillingteam_id,

        -- metadata
        m._fivetran_synced,
        m._loaded_at

    from meters m
    left join completions c
        on m.location_merrick_id = c.merrick_id

),

final as (

    select * from enriched

)

select * from final

