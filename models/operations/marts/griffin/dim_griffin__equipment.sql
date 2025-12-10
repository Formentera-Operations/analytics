{{
    config(
        materialized='table',
        tags=['griffin', 'marts', 'crescent']
    )
}}

/*
    Equipment dimension for Griffin namespace.
    Pumps, compressors, and other field equipment.
*/

with

equipment as (

    select * from {{ ref('int_griffin__equipment_enriched') }}
    -- Note: Procount uses non-standard flag values - filter downstream if needed

),

final as (

    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['equipment_merrick_id']) }} as equipment_key,
        
        -- natural key
        equipment_merrick_id,

        -- parent relationship
        location_merrick_id as completion_merrick_id,
        parent_well_name as well_name,
        parent_completion_name as completion_name,
        parent_wellpluscompletion_name as wellpluscompletion_name,

        -- equipment identification
        equipment_name,
        equipment_number,
        equipmentdescription,
        equipment_type,
        serial_number,
        sku_number,
        make as manufacturer,
        model_number as model,

        -- hierarchy
        equipment_route_id as route_id,
        parent_route_name as route_name,
        property_number,
        battery_id,
        equipment_lease_id as lease_id,
        equipment_area_id as area_id,
        division_id,
        gathering_system_id,
        facility_id,
        plant_id,

        -- specs
        horsepower,
        numberofstages,

        -- rental
        equipmentowner_id,
        rent_date,
        baserentalcharges,
        maintenancecharges,
        insurancecharges,
        pumpercharges,
        rentaltermsmonths,
        purchaseoption,
        agreement_number,
        rentalcompanyunit_number,
        internalunit_number,

        -- status
        active_flag as is_active,
        delete_flag,

        -- dates
        install_date,
        startactive_date,
        endactive_date,
        recordcreation_date,

        -- allocation
        allocationtype_flag,
        allocationorder,
        allocauto_flag,

        -- product
        product_code,
        product_type,
        disposition_code,

        -- metadata
        _loaded_at

    from equipment

)

select * from final
