{{ config(
    enable= true,
    materialized='table',
    tags=['marts', 'facts']
) }}

WITH jobs as (
    Select
        "Job ID"
        ,"Job End Datetime"
        ,CAST("Job End Datetime" as date) AS "Job End Date"
        ,"Job Start Datetime"
        ,CAST("Job Start Datetime" as date) AS "Job Start Date"
        ,"Planned Start Datetime"
        ,"Primary Job Type"
        ,"Well ID"
        ,"Wellview Job Category"
    FROM {{ ref('int_wellview_job') }}
)

Select *
from jobs