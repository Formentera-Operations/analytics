{{
    config(
        materialized='table',
        tags=['griffin', 'marts', 'crescent']
    )
}}

/*
    Tank dimension for Griffin namespace.
    Oil and water storage tanks.
*/

with

tanks as (

    select * from {{ ref('int_griffin__tanks_enriched') }}
    -- Note: Procount uses non-standard flag values - filter downstream if needed

),

final as (

    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['tank_merrick_id']) }} as tank_key,
        
        -- natural key
        tank_merrick_id,

        -- parent relationship
        location_merrick_id as completion_merrick_id,
        parent_well_name as well_name,
        parent_completion_name as completion_name,
        parent_wellpluscompletion_name as wellpluscompletion_name,

        -- tank identification
        tank_name,
        tankdescription,
        tank_type,
        tankconstruction,
        tankgauge_type,
        serial_number,
        make as manufacturer,
        model_number as model,

        -- external IDs
        scada_id,
        engineering_id,
        accounting_id,
        production_id,
        purchasertank_id,
        operatortank_number,
        haulertank_number,

        -- hierarchy
        tank_route_id as route_id,
        parent_route_name as route_name,
        property_number,
        battery_id,
        tankbattery_id,
        mastertankbattery_id,
        tank_lease_id as lease_id,
        tank_area_id as area_id,
        division_id,
        gathering_system_id,
        pipeline_id,
        terminal_id,

        -- specs/capacity
        capacity,
        barrelsperinch,
        barrelsperquarterinch,
        topfeet,
        topinches,
        topquarter,
        toptotalinches,

        -- strapping/measurement
        shellexpansioncoefficient,
        shellreferencetemperature,
        laststrapping_date,
        standardgaugetime,
        watercutaveragedays,

        -- product
        product_code,
        product_type,

        -- business entities
        purchaserbe_id,
        purchaserbeloc,
        haulerbe_id,
        haulerbeloc,
        waterhauler,
        waterpurchaser,

        -- regulatory
        mmsmeteringpoint,
        mmsfmp_number,
        statepointofdisposition,
        tankbatteryrole,
        measurementpointrole,

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
        fifoallocation_flag,

        -- location
        latitude,
        longitude,

        -- metadata
        _loaded_at

    from tanks

)

select * from final
