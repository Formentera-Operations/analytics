{{
    config(
        materialized='table',
        tags=['drilling', 'mart', 'dimension']
    )
}}

with zones as (
    select * from {{ ref('stg_wellview__zones') }}
),

well_360 as (
    select
        wellview_id,
        eid
    from {{ ref('well_360') }}
    where wellview_id is not null
),

wellbores as (
    select
        wellbore_id,
        wellbore_sk
    from {{ ref('dim_wellbore') }}
),

joined as (
    select
        z.zone_sk,

        -- dimensional FKs
        wb.wellbore_sk,
        w360.eid,

        -- natural keys
        z.record_id as zone_id,
        z.well_id,
        z.wellbore_id,

        -- identity and classification
        z.zone_name,
        z.zone_code,
        z.zone_api_number,
        z.objective,
        z.formation,
        z.formation_layer,
        z.reservoir,
        z.current_status,
        z.current_status_date,
        z.data_source,

        -- depths
        z.top_depth_ft,
        z.bottom_depth_ft,
        z.reference_depth_ft,
        z.zone_thickness_ft,
        z.top_depth_tvd_ft,
        z.bottom_depth_tvd_ft,
        z.reference_depth_tvd_ft,

        -- production lifecycle
        z.first_production_date,
        z.last_production_date,
        z.abandon_date,
        z.estimated_on_production_date,
        z.estimated_last_production_date,
        z.estimated_abandonment_date,

        -- completion linkage
        z.last_completion_id,
        z.last_completion_table_key,

        -- derived flags for common filtering
        (
            z.abandon_date is not null
            or lower(coalesce(z.current_status, '')) like '%abandon%'
        ) as is_abandoned,
        (
            z.abandon_date is null
            and lower(coalesce(z.current_status, '')) not like '%abandon%'
        ) as is_not_abandoned,

        -- audit
        current_timestamp() as _loaded_at

    from zones as z
    left join well_360 as w360
        on z.well_id = w360.wellview_id
    left join wellbores as wb
        on z.wellbore_id = wb.wellbore_id
),

final as (
    select
        zone_sk,
        wellbore_sk,
        eid,
        zone_id,
        well_id,
        wellbore_id,
        zone_name,
        zone_code,
        zone_api_number,
        objective,
        formation,
        formation_layer,
        reservoir,
        current_status,
        current_status_date,
        data_source,
        top_depth_ft,
        bottom_depth_ft,
        reference_depth_ft,
        zone_thickness_ft,
        top_depth_tvd_ft,
        bottom_depth_tvd_ft,
        reference_depth_tvd_ft,
        first_production_date,
        last_production_date,
        abandon_date,
        estimated_on_production_date,
        estimated_last_production_date,
        estimated_abandonment_date,
        last_completion_id,
        last_completion_table_key,
        is_abandoned,
        is_not_abandoned,
        _loaded_at
    from joined
)

select * from final
