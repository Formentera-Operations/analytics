{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'facts']
) }}

with jobs as (
    select
        afe_amount as "AFE Amount",
        afe_cost_type as "AFE Cost Type",
        afe_number as "AFE Number",
        afe_total_amount as "AFE Total Amount",
        api_10_number as "API 10 Number",
        duration_start_to_end_days as "Duration Start To End Days",
        cast(job_end_at as date) as "Job End Date",
        cast(calculated_end_at as date) as "Job End Date Calculated",
        job_end_at as "Job End Datetime",
        calculated_end_at as "Job End Datetime Calculated",
        job_id as "Job ID",
        job_objective as "Job Objective",
        cast(job_start_at as date) as "Job Start Date",
        job_start_at as "Job Start Datetime",
        planned_start_at as "Planned Start Datetime",
        job_type_primary as "Primary Job Type",
        job_type_secondary as "Secondary Job Type",
        time_log_total_hours as "Time Log Total Hours",
        total_field_estimate as "Total Field Estimate",
        well_id as "Well ID",
        job_category as "Wellview Job Category",
        well_code as "Well Code",
        well_type as "Well Type"
    from {{ ref('int_wellview_job') }}
)

select *
from jobs
where "Job Start Date" > last_day(dateadd(year, -3, current_date()), year)
