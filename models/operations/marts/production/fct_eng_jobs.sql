{{ config(
    enable= true,
    materialized='table',
    tags=['marts', 'facts']
) }}

WITH jobs as (
    Select
        "Job ID"
        ,"Job End Datetime"
        ,"Job Start Datetime"
        ,"Planned Start Datetime"
        ,"Primary Job Type"
        ,"Well ID"
        ,"Wellview Job Category"
    FROM {{ ref('int_wellview_job') }}
)

Select *
from jobs