{{
    config(
        materialized='table',
        tags=['griffin', 'marts', 'crescent']
    )
}}

/*
    Meter dimension for Griffin namespace.
    Gas and liquid measurement devices.
*/

with

meters as (

    select * from {{ ref('int_griffin__meters_enriched') }}
    -- Note: Procount uses non-standard flag values - filter downstream if needed

),

final as (

    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['meter_merrick_id']) }} as meter_key,
        
        -- natural key
        meter_merrick_id,

        -- parent relationship
        location_merrick_id as completion_merrick_id,
        parent_well_name as well_name,
        parent_completion_name as completion_name,
        parent_wellpluscompletion_name as wellpluscompletion_name,

        -- meter identification
        meter_name,
        meter_number,
        meterdescription,
        meter_type,
        serial_number,
        sku_number,
        make as manufacturer,
        model_number as model,

        -- external IDs
        scada_id,
        engineering_id,
        accounting_id,
        production_id,
        operatormeter_id,
        purchasermeter_id,

        -- hierarchy
        meter_route_id as route_id,
        parent_route_name as route_name,
        property_number,
        battery_id,
        meter_lease_id as lease_id,
        meter_area_id as area_id,
        division_id,
        meter_gathering_system_id as gathering_system_id,
        gathering_system_name,
        pipeline_id,
        terminal_id,

        -- specs
        absolutepressure,
        odometermaximum,
        metertestfrequency,

        -- product
        product_code,
        product_type,
        disposition_code,

        -- business entities
        purchaserbe_id,
        purchaserbeloc,
        gathererbe_id,
        transporterbe_id,

        -- regulatory
        regulatoryfacility_id,
        regulatoryfacility_type,
        statepointofdisposition,
        statefiling_flag,

        -- status
        active_flag as is_active,
        delete_flag,

        -- dates
        start_date,
        end_date,
        startactive_date,
        endactive_date,
        recordcreation_date,

        -- allocation
        allocationtype_flag,
        allocationorder,
        allocauto_flag,
        measurementpointrole,

        -- location
        latitude,
        longitude,
        regionarea,

        -- metadata
        _loaded_at

    from meters

)

select * from final
