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
        {{ dbt_utils.generate_surrogate_key(['j."Job ID"']) }} as job_sk,

        -- Well FK (via well_360.wellview_id)
        w360.eid,

        -- Natural key
        j."Job ID" as job_id,
        j."Well ID" as well_id,
        j."Wellbore ID" as wellbore_id,

        -- Job classification
        j."Primary Job Type" as job_type_primary,
        j."Secondary Job Type" as job_type_secondary,
        j."Wellview Job Category" as job_category,
        j."Complexity Index" as complexity_index,
        j."Job Objective" as job_objective,
        j."Geological Objective" as geological_objective,
        j."Target Formation" as target_formation,

        -- Key dates
        j."Job Start Datetime" as job_start_at,
        j."Job End Datetime" as job_end_at,
        j."Spud Datetime" as spud_at,
        j."Planned Start Datetime" as planned_start_at,
        j."Calculated End Datetime" as calculated_end_at,

        -- Duration
        j."Duration Start To End Days" as duration_days,

        -- Depths (already in FT from staging)
        j."Target Depth Ft" as target_depth_ft,
        j."Total Depth Reached Ft" as total_depth_ft,
        j."Depth Drilled Ft" as depth_drilled_ft,

        -- Drilling performance (already converted in staging)
        j."Rop Ft Per hr" as rop_ft_per_hr,
        j."Drilling Time Hours" as drilling_time_hours,
        j."Rotating Time Hours" as rotating_time_hours,
        j."Sliding Time Hours" as sliding_time_hours,
        j."Tripping Time Hours" as tripping_time_hours,
        j."Circulating Time Hours" as circulating_time_hours,

        -- Cost summary
        j."AFE Number" as afe_number_primary,
        j."AFE Amount" as afe_amount_primary,
        j."Total Field Estimate" as total_field_estimate,

        -- Status
        j."Primary Status" as status_primary,
        j."Secondary Status" as status_secondary,
        j."Technical Result" as technical_result,

        -- Rig enrichment (latest rig)
        r.rig_contractor,
        r.rig_number,
        r.rig_type,
        r.rig_category,
        r.rig_start_datetime as rig_start_at,
        r.rig_end_datetime as rig_end_at,

        -- Flags
        (j."Job End Datetime" is null) as is_active,

        -- Audit
        current_timestamp() as _loaded_at

    from jobs as j
    left join well_360 as w360
        on j."Well ID" = w360.wellview_id
    left join rigs as r
        on j."Job ID" = r.job_id
)

select * from joined
