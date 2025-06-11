{{ config(materialized='table') }}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2000-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
)

select 
    date_day,
    extract(year from date_day) as year,
    extract(quarter from date_day) as quarter,
    extract(month from date_day) as month,
    extract(day from date_day) as day,
    extract(dayofweek from date_day) as day_of_week,
    extract(dayofyear from date_day) as day_of_year,
    
    -- Fiscal periods (December year-end for your companies)
    extract(year from date_day) as fiscal_year,
    
    case 
        when extract(month from date_day) in (1,2,3) then 1
        when extract(month from date_day) in (4,5,6) then 2  
        when extract(month from date_day) in (7,8,9) then 3
        else 4
    end as fiscal_quarter,
    
    extract(month from date_day) as fiscal_month,
    
    -- Period calculations
    date_trunc('month', date_day) as month_start_date,
    date_trunc('quarter', date_day) as quarter_start_date,
    date_trunc('year', date_day) as year_start_date,
    
    -- Business day flags
    case 
        when extract(dayofweek from date_day) in (1, 7) then false 
        else true 
    end as is_weekday,
    
    current_timestamp as dim_created_at
    
from date_spine