{{
    config(
        materialized='view',
        tags=['griffin', 'intermediate', 'crescent']
    )
}}

/*
    Intermediate model enriching completions with decoded reference data.
    Joins producing status, producing methods, routes, and gathering systems.
*/

with

completions as (

    select * from {{ ref('stg_procount__completions') }}

),

producing_status as (

    select
        producing_status_id,
        producing_description,
        producing_description_short
    from {{ ref('stg_procount__producing_status') }}

),

producing_methods as (

    select
        producing_methods_id,
        producing_methods_description
    from {{ ref('stg_procount__producing_methods') }}

),

routes as (

    select
        route_id,
        route_name
    from {{ ref('stg_procount__routes') }}

),

gathering_systems as (

    select
        gathering_system_id,
        gathering_system_name
    from {{ ref('stg_procount__gathering_systems') }}

),

enriched as (

    select
        -- identifiers
        c.merrick_id,
        c.location_merrick_id,
        c.propertywell_id,
        c.gathering_system_id,
        c.route_id,
        c.battery_id,
        c.lease_id,
        c.area_id,
        c.division_id,
        c.group_id,
        c.platform_id,
        c.formation_id,
        c.reservoir_id,

        -- well/completion identifiers
        c.well_name,
        c.well_number,
        c.completion_name,
        c.completion_number,
        c.wellpluscompletion_name,
        c.uwi,
        c.uwidisplay,
        c.apiwell_number,
        c.apicompletion_number,
        c.stateapiwell_number,

        -- geography
        c.latitude,
        c.longitude,
        c.statelease_name,
        c.stateproducingzone,
        c.statedistrict_number,

        -- status codes with descriptions
        c.producingstatus,
        ps.producing_description as producing_status_description,
        ps.producing_description_short as producing_status_short,
        c.completionstatus,
        c.lastproducingstatus,

        -- method codes with descriptions
        c.producingmethod,
        pm.producing_methods_description as producing_method_description,
        c.lastproducingmethod,

        -- completion attributes
        c.completion_type,
        c.completionflowdirection,
        c.fromdepth,
        c.todepth,
        c.packerdepth,
        c.waterdepth,

        -- route and gathering system names
        r.route_name,
        gs.gathering_system_name,

        -- key dates
        c.dateofinitialproductionoil as first_oil_date,
        c.dateofinitialproductiongas as first_gas_date,
        c.datecompleted as completion_date,
        c.datewellacquired as acquisition_date,
        c.datewellsold as sold_date,
        c.dateshutin as shutin_date,
        c.dateabandoned as abandoned_date,
        c.startactive_date,
        c.endactive_date,
        c.lastproduction_date,
        c.lasttest_date,

        -- accounting
        c.property_number,
        c.accounting_id,
        c.production_id,
        c.engineering_id,
        c.operatorentity_id,
        c.operatorentityloc,

        -- flags
        c.active_flag,
        c.delete_flag,
        c.outsideoperated_flag,
        c.offshore_flag,

        -- reserves
        c.ultimatereservesoil,
        c.ultimatereservesgas,
        c.ultimatereserveswater,

        -- metadata
        c._fivetran_synced,
        c._loaded_at

    from completions c
    left join producing_status ps
        on c.producingstatus = ps.producing_status_id
    left join producing_methods pm
        on c.producingmethod = pm.producing_methods_id
    left join routes r
        on c.route_id = r.route_id
    left join gathering_systems gs
        on c.gathering_system_id = gs.gathering_system_id

),

final as (

    select
        -- identifiers
        merrick_id,
        location_merrick_id,
        propertywell_id,
        gathering_system_id,
        route_id,
        battery_id,
        lease_id,
        area_id,
        division_id,
        group_id,
        platform_id,
        formation_id,
        reservoir_id,

        -- well/completion identifiers
        well_name,
        well_number,
        completion_name,
        completion_number,
        wellpluscompletion_name,
        uwi,
        uwidisplay,
        apiwell_number,
        apicompletion_number,
        stateapiwell_number,

        -- geography
        latitude,
        longitude,
        statelease_name,
        stateproducingzone,
        statedistrict_number,

        -- status
        producingstatus,
        producing_status_description,
        producing_status_short,
        completionstatus,
        lastproducingstatus,

        -- method
        producingmethod,
        producing_method_description,
        lastproducingmethod,

        -- completion attributes
        completion_type,
        completionflowdirection,
        fromdepth,
        todepth,
        packerdepth,
        waterdepth,

        -- route and gathering system
        route_name,
        gathering_system_name,

        -- key dates
        first_oil_date,
        first_gas_date,
        completion_date,
        acquisition_date,
        sold_date,
        shutin_date,
        abandoned_date,
        startactive_date,
        endactive_date,
        lastproduction_date,
        lasttest_date,

        -- accounting
        property_number,
        accounting_id,
        production_id,
        engineering_id,
        operatorentity_id,
        operatorentityloc,

        -- flags
        active_flag,
        delete_flag,
        outsideoperated_flag,
        offshore_flag,

        -- reserves
        ultimatereservesoil,
        ultimatereservesgas,
        ultimatereserveswater,

        -- metadata
        _fivetran_synced,
        _loaded_at

    from enriched

)

select * from final
