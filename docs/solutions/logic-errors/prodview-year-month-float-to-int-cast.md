---
title: "ProdView Year/Month Stored as FLOAT in Source Tables — Cast to INT in Mart"
category: logic-errors
tags: [prodview, type-casting, float, integer, year, month, facility-monthly, date-trunc]
module: operations/marts/well_360
symptoms:
  - calendar year appears as 2026.0 instead of 2026 in mart query results
  - calendar month appears as 2.0 instead of 2 in BI tool
  - GROUP BY on year/month produces unexpected float values
  - period_start_date appears as a timestamp with time component when used as a date grain key
date_solved: 2026-02-19
---

# ProdView Year/Month Stored as FLOAT in Source Tables — Cast to INT in Mart

## Problem

`fct_facility_monthly` carries `calculation_year` and `calculation_month` columns. In BI tools and downstream queries, these appeared as `2026.0` and `2.0` rather than `2026` and `2`.

Additionally, `period_start_date` appeared as a `timestamp_ntz` value with time component (`2025-01-01 00:00:00`) rather than a clean date, making it awkward as a grain key.

## Root Cause

ProdView source tables store calendar year and month as FLOAT columns (double precision), not integer. This is a ProdView data model quirk — year/month are numeric fields without type constraints. The staging model carries these through as-is:

```sql
-- In stg_prodview__facility_monthly_volumes.sql
year::float as calculation_year,
month::float as calculation_month,
dttmstart::timestamp_ntz as period_start_date,
```

All three are technically correct at staging — staging preserves source types. But at the mart layer, these should be cast to their intended analytical types.

## Solution

Cast in the mart's source CTE:

```sql
-- In the facility_monthly CTE of fct_facility_monthly.sql
calculation_year::int as calculation_year,
calculation_month::int as calculation_month,
period_start_date::date as period_start_date,
period_end_date::date as period_end_date,

-- Derive a clean month date for use as the time grain key
date_trunc('month', period_start_date)::date as facility_month,
```

The `::date` cast on `date_trunc` is important — `date_trunc` returns a `timestamp_ntz` in Snowflake, not a `date`. Without the explicit cast, the grain key is a timestamp, which creates unnecessary complexity in BI tools and GROUP BY clauses.

## Where This Appears

Affects multiple ProdView calc tables:
- `PVT_PVFACILITYMONTHCALC` — `year`, `month` columns
- `PVT_PVUNITALLOCMONTH` — similar year/month fields
- Any ProdView "MonthCalc" table with year/month breakdown

Always check the staging model's column types before writing mart-level date logic.

## Prevention

1. **Cast year/month from ProdView to INT** at the mart layer. Staging preserves the source float type; marts should normalize to integers for analytical use.
2. **Always cast date_trunc to ::date** when using it as a grain key — `date_trunc` returns `timestamp_ntz` in Snowflake, not `date`.
3. **Use a derived `*_month` column** (date_trunc to month, cast to date) rather than a raw `period_start_date` as the time grain key in monthly facts. This makes the grain intent explicit and produces clean `YYYY-MM-01` dates.

## Related

- `models/operations/marts/well_360/fct_facility_monthly.sql`
- `models/operations/staging/prodview/facilities/stg_prodview__facility_monthly_volumes.sql`
