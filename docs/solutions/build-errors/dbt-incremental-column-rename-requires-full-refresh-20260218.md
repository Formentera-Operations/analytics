---
module: ODA Staging
date: 2026-02-18
problem_type: build_error
component: database
symptoms:
  - "Database Error: Numeric value 'OPTIONAL  : Optional' is not recognized"
  - "int_gl_enhanced incremental build fails after staging column rename"
  - "on_schema_change='sync_all_columns' does not resolve the error"
root_cause: config_error
resolution_type: config_change
severity: high
tags: [dbt, incremental, column-rename, full-refresh, on_schema_change, sync_all_columns, snowflake]
---

# Troubleshooting: dbt Incremental Model Fails After Column Rename

## Problem

When a staging model renames a column (e.g., `is_currency_missing` → `is_currency_defaulted`)
and a downstream incremental model references it, the incremental build fails with a cryptic
Snowflake error. Setting `on_schema_change='sync_all_columns'` is NOT sufficient to handle
column renames in incremental models.

## Environment

- Module: ODA Staging (stg_oda__gl → int_gl_enhanced)
- Affected Component: dbt incremental model with `on_schema_change='sync_all_columns'`
- Date: 2026-02-18

## Symptoms

- `Database Error: 100038 (22018): Numeric value 'OPTIONAL  : Optional' is not recognized`
- `int_gl_enhanced` incremental build fails after `stg_oda__gl` refactor
- The staging model itself builds and returns correct data (verified via `dbt show`)
- `on_schema_change='sync_all_columns'` is set but does not prevent the error
- Error appears to come from the Snowflake schema migration step, not from query logic

## What Didn't Work

**Attempted: Use `on_schema_change='sync_all_columns'`**
- **Why it failed:** `sync_all_columns` handles column additions and removals, but does NOT
  handle column TYPE changes or RENAMES. A rename is treated as: drop the old column name +
  add the new column name. During the schema migration step, Snowflake may encounter type
  mismatches or metadata issues that produce cryptic errors.

**Attempted: Verify staging model data is correct**
- Confirmed: `dbt show --select stg_oda__gl` returned correct data with `is_currency_defaulted`
- The error was not from the staging view itself
- The error was from the incremental merge step on the existing materialized table

## Solution

Run the incremental model with `--full-refresh` after any column rename in an upstream
staging model:

```bash
dbt run --select int_gl_enhanced --full-refresh
```

For large tables (180M row GL table), schedule during off-peak hours on M warehouse:

```bash
dbt run --select int_gl_enhanced --full-refresh --target prod
```

## Why This Works

`on_schema_change='sync_all_columns'` only handles:
- ✅ New columns added to the model → ALTERs the table to add them
- ✅ Columns removed from the model → DROPs them from the table

It does NOT handle:
- ❌ Column renames (treated as remove old + add new, but the migration logic can fail)
- ❌ Column type changes (e.g., INTEGER → BOOLEAN)

When a column is renamed in the staging model:
1. The existing incremental table still has the OLD column name
2. The new model output has the NEW column name
3. Snowflake's `sync_all_columns` tries to: DROP `is_currency_missing`, ADD `is_currency_defaulted`
4. The DROP or ADD may fail with a cryptic error depending on the column type and Snowflake's
   internal representation

`--full-refresh` drops and fully recreates the table from scratch, bypassing the incremental
merge entirely. This is the correct approach after any column rename.

## Prevention

1. **Whenever you rename a column in a staging model**, add a PR note:
   > ⚠️ Post-deploy: `dbt run --select [downstream_incremental_model] --full-refresh`

2. **Check for incremental downstream models** after any staging column rename:
   ```bash
   grep -r "materialized='incremental'" models/ | xargs grep -l "ref('stg_oda__gl')"
   ```

3. **Document the renamed column** in the PR body so reviewers know to schedule the
   full-refresh after merge.

4. **For the GL model specifically**, `int_gl_enhanced` and `int_general_ledger_enhanced`
   are both incremental — both may need `--full-refresh` after any GL staging column rename.

5. **Avoid column renames in staging models** if possible. If a rename is unavoidable,
   prefer using an alias to maintain backward compat:
   ```sql
   -- Backward compat: expose both old and new name for one sprint
   is_currency_defaulted,
   is_currency_defaulted as is_currency_missing  -- deprecated alias, remove next sprint
   ```

## Related Issues

- See also: [drilling-mart-sprint-2-fact-table-patterns.md](./drilling-mart-sprint-2-fact-table-patterns.md) — incremental vs table materialization decisions
- See also: [ci-defer-stale-column-names.md](./ci-defer-stale-column-names.md) — column name changes in CI defer context
