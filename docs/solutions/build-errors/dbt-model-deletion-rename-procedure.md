---
title: "Safe dbt Model Deletion and Rename Procedure"
category: build-errors
tags: [dbt, snowflake, model-deletion, model-rename, staging, cleanup, sqlfluff]
module: operations/staging
symptoms:
  - Duplicate staging models sourcing from the same table
  - Filename typos in model names
  - SQLFluff ST06 failures on commit after rename
date_solved: 2026-02-12
---

# Safe dbt Model Deletion and Rename Procedure

## Problem

Two issues in the ProdView staging layer needed cleanup before a larger refactor:

1. **Duplicate model:** `stg_prodview__flownet_header.sql` sourced from the same table (`PVT_PVFLOWNETHEADER`) as `stg_prodview__networks.sql`. The `flownet_header` version used legacy quoted aliases (`"Flow Net ID"`) while `networks` used snake_case — making `networks` the canonical version.

2. **Filename typo:** `stg_prodview__node_monthy_volumes.sql` (missing the "l" in "monthly").

Both models had zero downstream consumers (`ref()` calls), making the changes safe.

## Investigation Steps

### 1. Verify zero downstream consumers

```bash
# Search for any ref() calls to the models being changed
grep -r "ref.*flownet_header" models/ --include="*.sql"
grep -r "ref.*node_monthy" models/ --include="*.sql"

# Search YAML files for schema definitions or test references
grep -r "stg_prodview__flownet_header" models/ --include="*.yml"
grep -r "stg_prodview__node_monthy_volumes" models/ --include="*.yml"
```

All searches returned zero results — safe to proceed.

### 2. Verify source YAML is unaffected

The `PVT_PVFLOWNETHEADER` source entry in `src_prodview.yml` is shared between both models that source from it. Since `stg_prodview__networks.sql` still references it, the source definition stays. No YAML changes needed.

### 3. Check for exposure definitions

No exposure files exist in this project, so no BI tool references are at risk.

## Root Cause

Legacy models created during initial ProdView staging setup were never cleaned up. The `flownet_header` model was an early draft that was superseded by the better-structured `networks` model but never removed. The typo in `node_monthy_volumes` was a simple spelling error during initial creation.

## Solution

### Step 1: Delete the duplicate

```bash
git rm models/operations/staging/prodview/stg_prodview__flownet_header.sql
```

### Step 2: Rename the typo

```bash
git mv models/operations/staging/prodview/stg_prodview__node_monthy_volumes.sql \
       models/operations/staging/prodview/stg_prodview__node_monthly_volumes.sql
```

### Step 3: Validate

```bash
dbt parse --warn-error          # Syntax check
dbt build --select stg_prodview__networks stg_prodview__node_monthly_volumes
```

### Step 4: Post-merge Snowflake cleanup

dbt does NOT auto-drop Snowflake objects for deleted or renamed models. After merging, run in Snowflake:

```sql
DROP VIEW IF EXISTS FO_STAGE_DB.STG_PRODVIEW.STG_PRODVIEW__FLOWNET_HEADER;
DROP VIEW IF EXISTS FO_STAGE_DB.STG_PRODVIEW.STG_PRODVIEW__NODE_MONTHY_VOLUMES;
```

This applies to any dbt model deletion or rename — always check for orphaned objects in the target schema.

## Gotcha: SQLFluff ST06 Forces Column Reordering

When committing the renamed file, the pre-commit SQLFluff hook failed with:

```
L:  12 | P:   5 | ST06 | Select wildcards then simple targets before calculations
                       | and aggregates. [structure.column_order]
```

**ST06** requires simple column aliases (e.g., `idrec as node_calculation_id`) to appear before calculated expressions (e.g., `volhcliq / 0.158987294928 as gathered_hcliq_bbl`) in a SELECT statement.

The fix was to reorder the SELECT: move simple aliases (identifiers, dates, facility refs, system fields, Fivetran fields) before the unit conversion calculations. This made the diff noisier than a pure rename, but was required to pass the pre-commit hook.

**Lesson:** When renaming a file, expect SQLFluff to lint the "new" file against all rules, even if the content hasn't changed. Budget for lint fixes on any file that touches the staging area.

## Prevention

1. **Before deleting a model:** Always grep for `ref()` calls AND check YAML files for schema/test definitions.
2. **Before renaming a model:** Same checks, plus verify no BI tools query the Snowflake view directly.
3. **After merging deletions/renames:** Drop orphaned Snowflake views/tables. Consider adding a post-deploy runbook step.
4. **Naming review:** Catch typos during PR review of new models. Consider adding a model-naming linter.

## Related

- Sprint plan: `.claude/plans/peaceful-sparking-cascade.md`
- Prior art: commits `e71fbaf` (remove unused stg_oda__interesttype), `6559781` (remove income statement layer), `46e2871` (remove legacy ProdView models)
