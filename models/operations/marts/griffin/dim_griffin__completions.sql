{{
    config(
        materialized='table',
        tags=['griffin', 'marts', 'crescent']
    )
}}

/*
    Completion dimension for Griffin namespace.
    Slim, reporting-focused view of completion attributes.
*/

with

completions as (

    select * from {{ ref('int_griffin__completions_enriched') }}
    -- Note: Procount uses non-standard flag values (delete_flag=2 means active)
    -- Filter downstream if needed based on producing_status or other criteria

),

final as (

    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['merrick_id']) }} as completion_key,
        
        -- natural key
        merrick_id,

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

        -- hierarchy keys
        location_merrick_id,
        propertywell_id,
        route_id,
        gathering_system_id,
        battery_id,
        lease_id,
        area_id,
        division_id,
        formation_id,
        reservoir_id,

        -- hierarchy names
        route_name,
        gathering_system_name,

        -- geography
        latitude,
        longitude,
        statelease_name,
        stateproducingzone,
        statedistrict_number,

        -- status
        producingstatus as producing_status_code,
        producing_status_description,
        producing_status_short,
        completionstatus as completion_status_code,

        -- method
        producingmethod as producing_method_code,
        producing_method_description,

        -- completion attributes
        completion_type,
        completionflowdirection as completion_flow_direction,
        fromdepth as perf_top_depth,
        todepth as perf_bottom_depth,

        -- accounting
        property_number,
        accounting_id,
        operatorentity_id as operator_entity_id,

        -- key dates
        first_oil_date,
        first_gas_date,
        completion_date,
        acquisition_date,
        sold_date,
        shutin_date,
        abandoned_date,
        lastproduction_date as last_production_date,

        -- flags
        active_flag as is_active,
        outsideoperated_flag as is_outside_operated,
        offshore_flag as is_offshore,

        -- reserves
        ultimatereservesoil as eur_oil,
        ultimatereservesgas as eur_gas,
        ultimatereserveswater as eur_water,

        -- metadata
        _loaded_at

    from completions

)

select * from final
