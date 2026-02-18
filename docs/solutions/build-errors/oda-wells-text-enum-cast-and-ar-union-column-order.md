---
title: "ODA Wells AFEUSAGETYPE text enum cast failure and AR UNION column order mismatch"
category: build-errors
tags: [snowflake, oda-staging, pipeline, numeric-cast, union-all, type-mismatch, wells, ar]
module: operations/staging/oda, operations/intermediate/finance
symptoms:
  - "int_gl_enhanced fails: Numeric value 'OPTIONAL  : Optional' is not recognized"
  - "int_oda_ar_all_details fails: inconsistent data type for result columns for set operator input branches, expected TIMESTAMP_NTZ(9), got NUMBER(21,5)"
root_cause: |
  (1) stg_oda__wells casts AFEUSAGETYPE::int but the source column contains text enum labels.
  (2) int_oda_ar_all_details uses SELECT * UNION ALL across 4 models with different column orders.
date_solved: 2026-02-18
---

# ODA Wells AFEUSAGETYPE text enum cast failure and AR UNION column order mismatch

Two unrelated pipeline errors surfaced after adding AR staging models to the `marts-finance-general-ledger` selector (PR #279). Both were latent bugs exposed by running models for the first time in production.

## Problem

### Error 1: int_gl_enhanced

```
Numeric value 'OPTIONAL  : Optional' is not recognized
```

The `int_gl_enhanced` model failed during the GL pipeline run (both incremental and full refresh attempts). The error pointed to a numeric cast somewhere in the query chain, but `int_gl_enhanced` itself has no `::float` casts — all casts are inside upstream staging views.

### Error 2: int_oda_ar_all_details

```
inconsistent data type for result columns for set operator input branches,
expected TIMESTAMP_NTZ(9), got NUMBER(21,5)
```

The `int_oda_ar_all_details` model failed immediately on its first-ever production run.

## Investigation

### Error 1: Finding the source of 'OPTIONAL : Optional'

The initial suspicion was the expense/revenue deck staging models (which had `::float` casts on interest columns). PR #278 added `try_to_double()` to 4 deck staging models, but the error **persisted** after that fix was deployed.

Exhaustive search of all `::float` cast columns in all staging models referenced by `int_gl_enhanced`:

| Source Table | Columns Checked | Result |
|---|---|---|
| GL (180M rows) | ACCRUALDATEKEY, CASHDATEKEY, GROSSVALUE, NETVALUE, GROSSVOLUME, NETVOLUME, all ::int columns | **Clean** |
| ODA_VOUCHER_V2 | SUBLEDGERTOTAL | **Clean** |
| ODA_BATCH_ODA_OWNER_V2 | MINIMUMJIBINVOICE, MINIMUMREVENUECHECK | **Clean** |
| ODA_BATCH_ODA_VENDOR_V2 | MINIMUMAPCHECK | **Clean** |
| ODA_BATCH_ODA_ACCOUNT_V2 | All ::int columns | **Clean** |

The breakthrough came from searching ALL columns (not just numeric-cast ones) using `OBJECT_CONSTRUCT(*)`:

```sql
with src as (
    select object_construct(*) as obj
    from ESTUARY_DB.ESTUARY_SCHEMA.ODA_BATCH_ODA_WELL
)
select 'wells' as tbl from src
where to_varchar(obj) like '%OPTIONAL%'
```

This returned results. Drilling into the specific column:

```sql
select AFEUSAGETYPE, AFEUSAGETYPEID, CODE, NAME
from ODA_BATCH_ODA_WELL
where to_varchar(AFEUSAGETYPE) like '%OPTIONAL%'
```

| AFEUSAGETYPE | AFEUSAGETYPEID | CODE | NAME |
|---|---|---|---|
| OPTIONAL  : Optional | 0 | 8040111451 | WATCH THIS 1208... |
| NEVER     : Never | 0 | ... | ... |

**All 9,356 wells** have text enum labels in `AFEUSAGETYPE`, not integers. The numeric equivalent lives in `AFEUSAGETYPEID` (which correctly uses `::int`).

The staging model `stg_oda__wells.sql` had `AFEUSAGETYPE::int` — a hard cast on a text column. Even though `int_gl_enhanced` never directly references `afe_usage_type`, Snowflake's column pruning is **non-deterministic** for complex queries with `SELECT *` CTEs. The incremental run evaluated the column; the full refresh sometimes pruned it.

### Error 2: AR UNION column order

The 4 upstream AR intermediate models define the same 18 columns but in **different orders**:

| Position 12-18 | int_oda_ar_invoice | int_oda_ar_payments | int_oda_ar_netting | int_oda_ar_adjustments |
|---|---|---|---|---|
| 12 | voucher_id | hold_billing | hold_billing | hold_billing |
| 13 | invoice_date | voucher_id | voucher_id | voucher_id |
| 14 | total_invoice_amount | invoice_type | invoice_type | invoice_date |
| 15 | hold_billing | invoice_date | invoice_date | total_invoice_amount |
| 16 | invoice_description | total_invoice_amount | sort_order | sort_order |
| 17 | invoice_type | sort_order | invoice_description | invoice_description |
| 18 | sort_order | invoice_description | total_invoice_amount | invoice_type |

`SELECT * UNION ALL` resolves by **position, not name**. At position 15, `int_oda_ar_payments` has `invoice_date` (TIMESTAMP_NTZ) while `int_oda_ar_adjustments` has `total_invoice_amount` (NUMBER) — causing the type mismatch.

## Solution

### Fix 1: stg_oda__wells.sql (line 60)

```sql
-- Before (broken)
AFEUSAGETYPE::int as afe_usage_type,

-- After (fixed)
trim(AFEUSAGETYPE)::varchar as afe_usage_type,
```

### Fix 2: int_oda_ar_all_details.sql

Replace `SELECT *` with explicit column lists in each UNION ALL branch:

```sql
select
    company_code, company_name, owner_id, owner_code, owner_name,
    well_id, well_code, well_name, invoice_number, invoice_id,
    invoice_type_id, voucher_id, invoice_date, total_invoice_amount,
    hold_billing, invoice_description, invoice_type, sort_order
from {{ ref('int_oda_ar_invoice') }}

union all

select
    company_code, company_name, owner_id, owner_code, owner_name,
    -- ... same explicit list for each branch
from {{ ref('int_oda_ar_payments') }}
-- ... repeat for netting and adjustments
```

Both fixes merged in PR #280.

## Prevention

1. **Never cast source columns as `::int` or `::float` without checking actual values.** Use `SELECT DISTINCT column` or check `information_schema.columns` for the true data type. ODA source columns that look like IDs may contain text enum labels (format: `'LABEL  : Label'`).

2. **Never use `SELECT *` in UNION ALL.** Always use explicit column lists. Column order differences across models are invisible until runtime and produce cryptic type mismatch errors.

3. **Use `OBJECT_CONSTRUCT(*)` to search all columns at once.** When a bad value is hiding and you've exhausted known numeric columns, wrap the source in a CTE:
   ```sql
   with src as (select object_construct(*) as obj from SOURCE_TABLE)
   select * from src where to_varchar(obj) like '%SEARCH_TERM%'
   ```

4. **Snowflake column pruning is non-deterministic.** A broken `::int` cast in a staging view can silently pass for months if the column is never directly referenced, then fail unpredictably when the query plan changes. Fix broken casts even if they "work" — they're landmines.

5. **ODA enum pattern: always prefer the `*ID` column.** When ODA exposes both `FIELDNAME` (text label) and `FIELDNAMEID` (integer), the text label may contain the format `'TYPE  : Description'`. Cast the `*ID` column as `::int` and the base column as `::varchar`.

## Related

- PR #280: fix(oda): correct wells AFEUSAGETYPE cast and AR union column order
- PR #279: fix(selectors): add AR, check registers, drilling, well-360, ORM to pipeline coverage
- PR #278: fix(oda): resolve Orchestra pipeline errors — column renames + defensive float casts
- `docs/solutions/build-errors/dbt-incremental-column-rename-requires-full-refresh-20260218.md` — related `int_gl_enhanced` failure pattern
- `docs/solutions/build-errors/snowflake-reserved-word-column-cast-failure.md` — related Snowflake cast gotcha
- `docs/solutions/refactoring/oda-staging-sprint-5-ar-patterns.md` — Sprint 5 AR staging patterns
