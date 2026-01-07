{{
    config(
        enabled=true,
        materialized='view'
    )
}}

WITH header as (
    select DISTINCT
        "Asset Company"
        , floor("Asset Company Code") as "Asset Company Code"
        ,"Asset Company Full Name"
    from {{ ref('int_fct_well_header') }}
),

company as (
    Select
        company_name as "Asset Company"
        ,floor(company_code) as "Asset Company Code"
        ,company_full_name as "Asset Company Full Name"
    from {{ ref('dim_companies') }}
),

tbl as (
    Select
        case
            when h."Asset Company" IS NULL then c."Asset Company"
            else h."Asset Company"
        end as "Asset Company"
        ,c."Asset Company Code"
        ,case
            when h."Asset Company Full Name" IS NULL then c."Asset Company Full Name"
            else h."Asset Company Full Name"
        end as "Asset Company Full Name"
    from company c
    LEFT JOIN header h 
    on c."Asset Company Code" = h."Asset Company Code"


)


Select
    *
From tbl
    Order by "Asset Company Code"
