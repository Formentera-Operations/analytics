{{ config(
    tags=['wiserock', 'wellview']
) }}

select * from {{ ref('stg_wiserock__wv_job_time_logs') }}