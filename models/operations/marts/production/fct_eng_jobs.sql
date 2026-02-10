{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'facts']
) }}

WITH jobs as (
    Select
        "AFE Amount"
        ,"AFE Cost Type"
        ,"AFE Number"
        ,"AFE Total Amount"
        ,"API 10 Number"
        ,"Duration Start To End Days"
        ,CAST("Job End Datetime" as date) AS "Job End Date"
        ,CAST("Calculated End Datetime" as date) AS "Job End Date Calculated"
        ,"Job End Datetime"
        ,"Calculated End Datetime" as "Job End Datetime Calculated"
        ,"Job ID"
        ,"Job Objective"
        ,CAST("Job Start Datetime" as date) AS "Job Start Date"
        ,"Job Start Datetime"
        ,"Planned Start Datetime"
        ,"Primary Job Type"
        ,"Secondary Job Type"
        ,"Time Log Total Hours"
        ,"Total Field Estimate"
        ,"Well ID"
        ,"Wellview Job Category"
        ,"Well Code"
        ,"Well Type"
    FROM {{ ref('int_wellview_job') }}
)

Select *
from jobs
where "Job Start Date" > LAST_DAY(DATEADD(year, -3,CURRENT_DATE()), year)