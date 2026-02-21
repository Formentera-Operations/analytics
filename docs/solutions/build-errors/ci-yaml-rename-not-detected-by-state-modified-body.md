---
title: "CI Fails After YAML-Only Model Rename — state:modified.body Doesn't Detect Name Changes"
category: build-errors
tags: [dbt, ci, defer, state-modified, model-rename, yaml, slim-ci]
module: operations/staging/oda/revenue
symptoms:
  - "Object 'FO_STAGE_DB.STG_ODA.STG_ODA__OWNER_REVENUE_DETAIL_V2' does not exist or not authorized"
  - CI tests fail with database error on renamed models
  - Models build locally but fail in CI after YAML name change
  - Multiple test ERRORs referencing the new model name in the production schema
date_solved: 2026-02-20
---

# CI Fails After YAML-Only Model Rename — state:modified.body Doesn't Detect Name Changes

## Problem

After renaming dbt models by changing the YAML `name:` field (e.g., `stg_oda__ownerrevenuedetail_v2` → `stg_oda__owner_revenue_detail_v2`), CI fails with 12 `Object does not exist` errors:

```
Database Error in test unique_stg_oda__owner_revenue_detail_v2_id
  002003 (42S02): SQL compilation error:
  Object 'FO_STAGE_DB.STG_ODA.STG_ODA__OWNER_REVENUE_DETAIL_V2' does not exist or not authorized.
```

The models build fine locally (dev schema) but fail in CI because the renamed models are never built in the CI schema.

## Root Cause

Our CI selector is:

```yaml
dbt build --select "state:modified.body+ state:modified.macros+ state:new+"
```

When you rename a model by **only changing the YAML `name:` field** without touching the SQL file:

1. **`state:modified.body`** compares raw Jinja SQL templates — unchanged SQL file = not detected
2. **`state:modified.macros`** compares upstream macro dependencies — no change = not detected
3. **`state:new`** sees the new model name but **the SQL file already existed on main** (it was committed in a prior PR with the new filename)

The result: dbt's state comparison doesn't flag the renamed model as needing a build. Tests run against the model (because the YAML was modified), but `--defer` resolves the model reference to the production schema where the new name doesn't exist yet.

### Why it's subtle

The scenario requires a specific sequence:

1. PR #292 creates both `stg_oda__ownerrevenuedetail_v2.sql` (old name) and its YAML entry
2. PR #301 creates `stg_oda__owner_revenue_detail_v2.sql` (new name, identical content) but doesn't delete the old file
3. PR #303 (this PR) deletes the old SQL file and updates the YAML `name:` to point to the new SQL file

In step 3, dbt sees:
- The SQL file `stg_oda__owner_revenue_detail_v2.sql` was **not modified** in this PR (it already existed)
- The YAML node name changed, triggering test execution
- But the model VIEW is never built in CI because `state:modified.body` doesn't flag unchanged SQL

## Solution

**Touch the SQL file** so `state:modified.body` detects a change and builds the model in the CI schema. A comment addition is the cleanest approach:

```sql
{#
    ...existing notes...
    - Renamed from stg_oda__ownerrevenuedetail_v2 to snake_case in M2 sprint
#}
```

This forces dbt to:
1. Detect the model as `state:modified.body` (SQL template changed)
2. Build the VIEW in `FO_CI_DB.DBT_CI_{PR}` schema
3. Run tests against the CI schema view (which now exists)

### Checklist for YAML-only renames

1. Delete the old SQL file
2. Update the YAML `name:` field to the new name
3. **Touch the new SQL file** — add a rename note in the header comment
4. Run `sqlfluff fix` on the touched file (pre-commit hooks will lint it fresh)
5. Verify with `dbt parse` locally
6. Push and confirm CI passes

## Prevention

1. **Always touch the SQL file when renaming via YAML.** Even a comment change ensures `state:modified.body` picks it up.
2. **Prefer renaming SQL file + YAML in the same PR** rather than creating a new file in one PR and renaming the YAML in another. The split-PR pattern creates the blindspot.
3. **Budget for sqlfluff fixes** when touching pre-existing files — the pre-commit hook will lint the "modified" file against all current rules, catching alignment issues (LT01) that were grandfathered in.

## Gotcha: sqlfluff LT01 cascade

When you touch a pre-existing SQL file to fix the rename, the pre-commit sqlfluff hook lints the entire file — not just your change. Files written with column-aligned `as` spacing (e.g., `ID::varchar                as id`) will fail LT01:

```
L:  34 | P:  40 | LT01 | Expected only single space before 'as' keyword.
```

Run `sqlfluff fix <file> --force` to auto-fix. This adds noise to the diff but is required to pass pre-commit hooks.

## Related

- [CI --defer Fails with 'Object does not exist'](ci-defer-object-does-not-exist.md) — the general `--defer` resolution problem (different root cause: wrong `--target` for prod artifacts)
- [Safe dbt Model Deletion and Rename Procedure](dbt-model-deletion-rename-procedure.md) — step-by-step rename/delete checklist (doesn't cover the CI `state:modified.body` blindspot)
- Fix: PR #303 (ODA Analytics M2 — Revenue Staging Sprint)
- CI selector docs: https://docs.getdbt.com/reference/node-selection/methods#the-state-method
