{{ config(
    enable= true,
    materialized='table',
    tags=['marts', 'facts']
) }}

WITH jobs as (
    Select
        "Duration Start To End Days"
        ,"Final Actual Cost"
        ,CAST("Job End Datetime" as date) AS "Job End Date"
        ,CAST("Calculated End Datetime" as date) AS "Job End Date Calculated"
        ,"Job End Datetime"
        ,"Calculated End Datetime" as "Job End Date Calculated"
        ,"Job ID"
        ,CAST("Job Start Datetime" as date) AS "Job Start Date"
        ,"Job Start Datetime"
        ,"Planned Start Datetime"
        ,"Primary Job Type"
        ,"Secondary Job Type"
        ,"Well ID"
        ,"Wellview Job Category"
    FROM {{ ref('int_wellview_job') }}
)

Select *
from jobs