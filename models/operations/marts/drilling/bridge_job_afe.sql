{{
    config(
        materialized='table',
        tags=['drilling', 'mart', 'bridge']
    )
}}

with afe_defs as (
    select * from {{ ref('stg_wellview__job_afe_definitions') }}
),

jobs as (
    select
        job_sk,
        job_id
    from {{ ref('dim_job') }}
),

joined as (
    select
        {{ dbt_utils.generate_surrogate_key(['a.job_afe_id']) }} as job_afe_sk,

        -- FKs
        j.job_sk,

        -- Natural keys
        a.job_afe_id,
        a.job_id,
        a.well_id,

        -- AFE identification
        a.afe_number,
        a.supplemental_afe_number,
        a.afe_type,
        a.afe_cost_type,
        a.afe_status,
        a.cost_type,

        -- Dates
        a.afe_date,
        a.afe_close_date,

        -- Working interest
        a.working_interest_percent,

        -- AFE amounts (gross — primary for drilling ops)
        a.total_afe_amount,
        a.total_afe_supplemental_amount,
        a.total_afe_plus_supplemental_amount,

        -- Field estimates
        a.total_field_estimate,

        -- Variances
        a.afe_minus_field_estimate_variance,

        -- Flags
        a.exclude_from_cost_calculations,

        -- Audit
        current_timestamp() as _loaded_at

    from afe_defs as a
    left join jobs as j
        on a.job_id = j.job_id

)

select * from joined
