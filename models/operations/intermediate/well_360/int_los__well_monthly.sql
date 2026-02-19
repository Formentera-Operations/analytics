{{
    config(
        materialized='ephemeral',
        tags=['intermediate', 'fo', 'well_360', 'los']
    )
}}

{#
    Intermediate: LOS Financials Rolled Up to Well-Month
    ======================================================

    PURPOSE:
    Aggregates fct_los transaction-level GL data to a monthly well grain,
    pivoting los_category into columns for production analytics consumption.

    GRAIN:
    One row per (eid, los_month). EID derived as right(well_code, 6).

    KEY DESIGN NOTES:
    - Filters to location_type = 'Well' to exclude facility/external records.
    - los_gross_amount is already pre-signed in fct_los (costs are negative).
      Summing directly gives net contribution per category.
    - los_net_income = SUM of all categories (revenue + signed expenses).
    - los_month = first day of the journal_month_start truncated to month.

    CONSUMERS:
    - fct_well_production_monthly (gold)
    - plat_well__performance_scorecard (platinum)
#}

with

los_well as (
    select
        date_trunc('month', journal_month_start)::date as los_month,
        los_category,
        los_gross_amount,
        right(well_code, 6) as eid
    from {{ ref('fct_los') }}
    where
        location_type = 'Well'
        and well_code is not null
),

aggregated as (
    select
        eid,
        los_month,

        -- revenue
        sum(
            case when los_category = 'Revenue' then los_gross_amount else 0 end
        ) as los_revenue,

        -- lease operating expenses
        sum(
            case
                when los_category = 'Lease Operating Expenses'
                    then los_gross_amount
                else 0
            end
        ) as los_loe,

        -- production and ad valorem taxes (severance tax)
        sum(
            case
                when los_category = 'Production & Ad Valorem Taxes'
                    then los_gross_amount
                else 0
            end
        ) as los_severance_tax,

        -- net income = sum of all pre-signed categories
        sum(los_gross_amount) as los_net_income

    from los_well
    group by all
)

select * from aggregated
