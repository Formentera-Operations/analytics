{{
    config(
        materialized='table',
        tags=['drilling', 'mart', 'dimension']
    )
}}

with jobs as (
    select * from {{ ref('stg_wellview__jobs') }}
),

well_360 as (
    select
        wellview_id,
        eid
    from {{ ref('well_360') }}
    where wellview_id is not null
),

-- Latest rig per job (by end date, then start date, then ID)
rigs as (
    select *
    from {{ ref('stg_wellview__rigs') }}
    qualify row_number() over (
        partition by job_id
        order by
            rig_end_datetime desc nulls last,
            rig_start_datetime desc nulls last,
            job_rig_id desc
    ) = 1
),

joined as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['j.job_id']) }} as job_sk,

        -- Well FK (via well_360.wellview_id)
        w360.eid,

        -- Natural key
        j.job_id,
        j.well_id,
        j.wellbore_id,

        -- Job classification
        j.job_type_primary,
        j.job_type_secondary,
        j.job_category,
        j.complexity_index,
        j.job_objective,
        j.geological_objective,
        j.target_formation,

        -- Key dates
        j.job_start_at,
        j.job_end_at,
        j.spud_at,
        j.planned_start_at,
        j.calculated_end_at,

        -- Duration
        j.duration_start_to_end_days as duration_days,

        -- Depths (already in FT from staging)
        j.target_depth_ft,
        j.total_depth_ft,
        j.depth_drilled_ft,

        -- Drilling performance (already converted in staging)
        j.rop_ft_per_hr,
        j.drilling_time_hours,
        j.rotating_time_hours,
        j.sliding_time_hours,
        j.tripping_time_hours,
        j.circulating_time_hours,

        -- Cost summary
        j.afe_number as afe_number_primary,
        j.afe_amount as afe_amount_primary,
        j.total_field_estimate,

        -- Status
        j.status_primary,
        j.status_secondary,
        j.technical_result,

        -- Rig enrichment (latest rig)
        r.rig_contractor,
        r.rig_number,
        r.rig_type,
        r.rig_category,
        r.rig_start_datetime as rig_start_at,
        r.rig_end_datetime as rig_end_at,

        -- Flags
        (j.job_end_at is null) as is_active,

        -- Audit
        current_timestamp() as _loaded_at

    from jobs as j
    left join well_360 as w360
        on j.well_id = w360.wellview_id
    left join rigs as r
        on j.job_id = r.job_id
)

select * from joined
