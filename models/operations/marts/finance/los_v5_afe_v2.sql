{{ config(
    enable= true,
    materialized='table'
) }}

Select 
*
from
{{ ref('int_oda_afe_v2') }}