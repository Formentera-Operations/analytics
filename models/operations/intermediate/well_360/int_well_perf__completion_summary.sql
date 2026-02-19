{{
    config(
        materialized='ephemeral'
    )
}}

with source as (
    select
        eid,
        job_id,
        actual_total_stages,
        proppant_total_calc_lb,
        volume_clean_total_calc_bbl,
        length_gross_ft
    from {{ ref('fct_stimulation') }}
    where eid is not null
),

aggregated as (
    select
        eid,
        count(distinct job_id) as stim_job_count,
        sum(actual_total_stages) as total_stages,
        sum(proppant_total_calc_lb) as total_proppant_lb,
        sum(volume_clean_total_calc_bbl) as total_clean_volume_bbl,
        -- Audit only: stimulated interval per job, summed across all jobs.
        -- Do NOT use this as the intensity denominator — use well_360.lateral_length_ft
        -- instead, since re-stimulations inflate this sum.
        sum(length_gross_ft) as stim_lateral_length_ft
    from source
    group by eid
),

final as (
    select * from aggregated
)

select * from final
