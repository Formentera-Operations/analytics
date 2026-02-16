{{
    config(
        materialized='table',
        cluster_by=['well_id']
    )
}}

with stg_wells as (
    select * from {{ ref('stg_cc__project_wells') }}
),

-- Calculate additional metrics or enhancements here
enhanced_wells as (
    select
        -- Well identifiers and IDs
        well_id,
        aries_id,
        phdwin_id,
        chosen_id,
        well_name,
        well_number,
        well_type,
        lease_name,
        api_10,
        api_12,
        api_14,

        -- Operator information
        operator,
        operator_code,
        is_operated,

        -- Well characteristics
        status,
        primary_product,

        -- Well technical details
        surface_latitude,
        surface_longitude,
        measured_depth,
        true_vertical_depth,
        lateral_length,

        -- Geographic location
        basin,
        county,
        state,

        -- Classification and categorization
        reserve_category,
        company_name,
        operator_category,

        -- Add derived fields
        data_pool,

        data_source,

        has_daily,

        has_monthly,

        -- Source metadata
        created_at,
        updated_at,
        _portable_extracted,
        case
            when lateral_length > 0 and true_vertical_depth > 0
                then true_vertical_depth + lateral_length
            else measured_depth
        end as calculated_total_depth,
        coalesce(lateral_length > 0, false) as is_horizontal,
        coalesce(status = 'ACTIVE' or status = 'PRODUCING', false) as is_active,
        case
            when primary_product = 'OIL' then 'Oil'
            when primary_product = 'GAS' then 'Gas'
            when primary_product = 'BOTH' or primary_product = 'OIL & GAS' then 'Oil & Gas'
            else coalesce(primary_product, 'Unknown')
        end as product_category,

        -- Processing metadata
        current_timestamp() as loaded_at

    from stg_wells
),

-- Add any data quality handling or enrichment here
final as (
    select
        *,
        -- Create a simple well classification
        case
            when is_horizontal and lateral_length >= 10000 then 'Long Lateral'
            when is_horizontal and lateral_length >= 5000 then 'Medium Lateral'
            when is_horizontal then 'Short Lateral'
            else 'Vertical'
        end as well_class,

        -- Create unified location string
        basin || ' - ' || county || ', ' || state as location_str

    from enhanced_wells
)

select * from final
