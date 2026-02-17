---
title: "Snowflake Reserved Word Column Names Cause Cast Failures"
category: build-errors
tags: [snowflake, reserved-words, quoting, cast, staging]
module: ODA Calendar
symptom: "SQL compilation error: invalid type [CAST(SOURCE.WEEK AS NUMBER(38,0))] for parameter 'TO_NUMBER'"
root_cause: "Unquoted Snowflake reserved words (WEEK, MONTH, YEAR, etc.) invoke built-in functions instead of referencing source columns"
---

# Snowflake Reserved Word Column Names Cause Cast Failures

## Problem

When a source table has columns named with Snowflake reserved words (e.g., `WEEK`, `MONTH`, `YEAR`, `DATE`, `DAY`, `QUARTER`), using them unquoted in SQL causes Snowflake to interpret them as **function calls** rather than column references.

## Symptoms

```
SQL compilation error:
invalid type [CAST(SOURCE.WEEK AS NUMBER(38,0))] for parameter 'TO_NUMBER'
```

This specific error occurs because:
1. `WEEK` (unquoted) → Snowflake calls `WEEK()` function → returns `DATE` type
2. `::int` cast on a `DATE` → fails with "invalid type for TO_NUMBER"

## Root Cause

Snowflake has built-in functions with the same names as common column names. When unquoted, Snowflake resolves these as functions, not column references:

| Unquoted | Interpreted As | Returns |
|----------|---------------|---------|
| `WEEK` | `WEEK()` function | Current week number (int) |
| `MONTH` | `MONTH()` function | Current month (int) |
| `YEAR` | `YEAR()` function | Current year (int) |
| `DATE` | `DATE()` function / type | Varies |
| `DAY` | `DAY()` function | Current day (int) |
| `QUARTER` | `QUARTER()` function | Current quarter (int) |

**Subtle trap**: An unquoted `WEEK::int` may appear to "work" because `WEEK()` returns the current week number as an integer. But it returns **the current week**, not the column value. This can go unnoticed if you don't validate the actual data.

## Solution

Double-quote all reserved word column names to force identifier resolution:

```sql
-- WRONG: Invokes WEEK() function, returns current week number
WEEK::int as week,

-- RIGHT: References the WEEK column from source table
"WEEK"::date as week,  -- noqa: RF06
```

Note: After quoting, the actual column type may differ from what you expected. In the MDM_CALENDAR case, the `WEEK` column turned out to be DATE type (first date of the week), not an integer.

### sqlfluff Suppression

sqlfluff RF06 rule flags quoted identifiers as "unnecessary". Add `-- noqa: RF06` to suppress:

```sql
"DATE"::date as date,       -- noqa: RF06
"DAY"::int as day,          -- noqa: RF06
"WEEK"::date as week,       -- noqa: RF06
"MONTH"::int as month,      -- noqa: RF06
"QUARTER"::int as quarter,  -- noqa: RF06
"YEAR"::int as year,        -- noqa: RF06
```

## Prevention

1. **Always check `information_schema.columns`** for actual data types before casting
2. **Always double-quote** column names that match Snowflake reserved words/function names
3. **Validate data** with `dbt show --limit 5` — don't trust that a model "works" without checking values
4. **Common reserved word columns**: DATE, DAY, WEEK, MONTH, QUARTER, YEAR, TIME, TIMESTAMP, ORDER, GROUP, TABLE, SELECT, FROM, WHERE

## Discovery

Found during ODA Sprint 2 refactoring of `stg_oda__calendar` (MDM_CALENDAR source table). The old model used unquoted `WEEK::int` which accidentally invoked the `WEEK()` function. After proper quoting, the column resolved to DATE type, requiring a `::date` cast instead of `::int`.
