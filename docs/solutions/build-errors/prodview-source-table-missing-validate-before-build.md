---
title: "ProdView Source Table Missing in Snowflake — dbt parse Passes but dbt build Fails"
category: build-errors
tags: [prodview, source-validation, fivetran, information-schema, dbt-parse, missing-table]
module: operations/staging/prodview
symptoms:
  - dbt parse passes cleanly with no errors
  - dbt build fails with "object does not exist or not authorized" on first Snowflake query
  - Model builds fine in dev but fails in CI because the table was never synced
  - Context documentation describes the source table but the table doesn't exist in Snowflake
date_solved: 2026-02-19
---

# ProdView Source Table Missing in Snowflake — dbt parse Passes but dbt build Fails

## Problem

During ProdView mart Sprint 3 planning, models were designed against `WVT_WVJRIGACTIVITYCALC` and `WVT_WVJRSTANDCALC` (WellView calc tables) based on context documentation. After building out the full models and YAML, `dbt build` failed:

```
Database Error: Object 'FO_RAW_DB.WELLVIEW.WVT_WVJRIGACTIVITYCALC' does not exist or not authorized.
```

Neither table had been synced by Fivetran. `WVT_WVJSTAND` existed but had 0 rows.

## Root Cause

`dbt parse` validates:
- Jinja/SQL syntax
- Model references (`{{ ref() }}`)
- Source declarations (`{{ source() }}`)

`dbt parse` does NOT validate:
- Whether the source table actually exists in Snowflake
- Whether the table has any rows
- Whether columns referenced in SQL actually exist in the table

ProdView's context documentation (`context/sources/prodview/`) describes what ProdView **can** expose to Fivetran — not what Fivetran has actually synced. Many ProdView tables exist in the schema definition but were never configured for sync, or were configured but have never received data.

## Solution

**Always query `information_schema.tables` before building any model against a NEW source table:**

```sql
select table_name, row_count
from information_schema.tables
where table_schema = 'FORMENTERAOPS_PV30_DBO'  -- ProdView schema
  and table_name in (
      'PVT_PVFACILITY',
      'PVT_PVUNITCOMPSTATUS',
      'PVT_PVUNITCOMPPARAM'
      -- add all tables you plan to use
  )
order by table_name
```

Run this via `dbt show --inline "..."` before writing any model code.

**Also check row counts** for tables that might exist but be empty:

```sql
select count(*) from FORMENTERAOPS_PV30_DBO.PVT_PVUNITCOMPSTATUS
```

An empty table still "exists" but a staging model built against it will produce 0 rows in all downstream facts.

## Snowflake schema locator

ProdView source schema (from `_src_prodview.yml`):

```yaml
database: PELOTON_FORMENTERAOPS_FORMENTERAOPS_PV30
schema: FORMENTERAOPS_PV30_DBO
```

WellView source schema (from `_src_wellview.yml`):

```yaml
database: PELOTON_FORMENTERAOPS_FORMENTERAOPS_WV30
schema: FORMENTERAOPS_WV30_DBO
```

## Prevention

1. **Validate before planning.** When a sprint plan references new ProdView/WellView source tables, run the `information_schema.tables` query as the first step — before writing any code.
2. **Validate before committing.** After building a new staging model, run `dbt show --select <model> --limit 5` to confirm real data returns. A model that compiles but returns 0 rows is a silent failure.
3. **CI doesn't catch this.** CI runs `dbt parse --warn-error` which only validates syntax. A missing source table only fails at `dbt build` time when Snowflake is queried.

## Related

- `context/sources/prodview/entity_model.md` — what ProdView can expose (not what's synced)
- `docs/reference/source-systems.md` — Fivetran source systems and schemas
- MEMORY.md "Source Table Validation (Critical)" entry
