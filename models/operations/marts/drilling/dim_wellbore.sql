{{
    config(
        materialized='table',
        tags=['drilling', 'mart', 'dimension']
    )
}}

with wellbores as (
    select * from {{ ref('stg_wellview__wellbores') }}
),

well_360 as (
    select
        wellview_id,
        eid
    from {{ ref('well_360') }}
    where wellview_id is not null
),

joined as (
    select
        {{ dbt_utils.generate_surrogate_key(['wb.record_id']) }} as wellbore_sk,

        -- Well FK (via well_360.wellview_id)
        w360.eid,

        -- Natural keys
        wb.record_id as wellbore_id,
        wb.well_id,
        wb.parent_wellbore_id,

        -- Identification
        wb.wellbore_name,
        wb.wellbore_api_uwi,
        wb.purpose,
        wb.profile_type,
        wb.current_status,
        wb.current_status_date,

        -- Depths (already in FT from staging)
        wb.start_depth_ft,
        wb.total_depth_ft as md_total_ft,
        wb.total_depth_tvd_ft as tvd_total_ft,
        wb.min_kickoff_depth_ft as kickoff_depth_ft,
        wb.draw_depth_ft,

        -- Directional
        wb.max_inclination_degrees,
        wb.max_dls_degrees_per_100ft,
        wb.max_departure_ft,
        wb.unwrapped_displacement_ft,
        wb.closure_direction_degrees,

        -- Dates
        wb.start_drill_date as spud_date,
        wb.end_drill_date as td_date,
        wb.duration_hours,

        -- Location (bottom hole)
        wb.latitude_degrees,
        wb.longitude_degrees,

        -- Flags
        case
            when wb.profile_type ilike '%horizontal%' then 'Horizontal'
            when wb.profile_type ilike '%directional%' then 'Directional'
            when wb.profile_type ilike '%vertical%' then 'Vertical'
            else wb.profile_type
        end as profile_category,

        -- Audit
        current_timestamp() as _loaded_at

    from wellbores as wb
    left join well_360 as w360
        on wb.well_id = w360.wellview_id
)

select * from joined
