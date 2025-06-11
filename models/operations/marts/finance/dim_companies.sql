{{ config(materialized='table') }}

select 
    id as company_id,
    code as company_code,
    name as company_name,
    full_name as company_full_name,
    fiscal_year_end_month,
    current_fiscal_year,
    is_partnership,
    current_timestamp as dim_created_at,
    current_timestamp as dim_updated_at
from {{ ref('stg_oda__company_v2') }}