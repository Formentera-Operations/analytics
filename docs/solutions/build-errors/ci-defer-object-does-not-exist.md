---
title: "CI --defer Fails with 'Object does not exist' on Unmodified Upstream Models"
category: build-errors
tags: [dbt, ci, defer, state-modified, snowflake, slim-ci, warehouse]
module: .github/workflows
symptoms:
  - "Object 'FO_CI_DB.DBT_CI_xxx.STG_*' does not exist or not authorized"
  - CI fails on view-materialized models that reference unmodified upstream models
  - Models that previously passed CI start failing after workflow changes
date_solved: 2026-02-12
---

# CI --defer Fails with 'Object does not exist' on Unmodified Upstream Models

## Problem

CI (`validate-dbt-changes`) fails when building view-materialized models that reference unmodified upstream models:

```
002003 (42S02): SQL compilation error:
  Object 'FO_CI_DB.DBT_CI_245.STG_PRODVIEW__UNITS' does not exist or not authorized.
```

The `--defer` flag should resolve unbuilt refs to production, but instead resolves to the CI schema where the object was never built.

## Root Cause

Two interacting issues in the CI workflow:

### 1. Prod artifacts generated with wrong target

Commit `0d9ed4e` changed prod artifact generation from `--target prod` to `--target ci` to fix a different problem (false `state:modified` positives). This meant the prod manifest recorded all model locations as `FO_CI_DB.DBT_CI_xxx.*` instead of their actual prod locations (`FO_STAGE_DB.stg_prodview.*`, etc.).

When `--defer` looked up an unbuilt model in the prod manifest, it got the CI schema location — where the object doesn't exist.

### 2. The original false-positive problem

The reason `--target ci` was used: the `set_warehouse_size` macro returns target-specific warehouse names (`DBT_PROD_WH_M` vs `DBT_CI_WH_M`). When prod artifacts used `--target prod` and the build used `--target ci`, dbt's `state:modified` flagged these as changed models even though only the warehouse config differed — not the SQL.

This caused 5+ models and all their downstream to be selected and built unnecessarily.

## Solution

Two changes in `.github/workflows/validate-dbt-changes.yaml`:

### 1. Revert prod artifacts to `--target prod`

```yaml
# Generate production artifacts
- name: Generate production artifacts
  run: >
    dbt parse
    --profile integration_tests
    --target prod              # NOT ci — refs must resolve to real prod objects
    --target-path prod-artifacts/
```

This ensures `--defer` resolves to actual prod Snowflake objects.

### 2. Use targeted `state:modified` subselectors instead of `state:modified+`

```yaml
- name: dbt build (incremental)
  run: >
    dbt build
    --select "state:modified.body+ state:modified.macros+ state:new+"
    --defer --state prod-artifacts/ --favor-state
    --indirect-selection cautious
    --profile integration_tests
    --target ci
```

`state:modified.body` (available since dbt 1.8) compares only the model's raw Jinja template, ignoring config-only diffs like `snowflake_warehouse`. `state:modified.macros` catches models whose upstream macro dependencies changed (important: `modified.body` only checks the template text, not compiled output — so macro-only PRs would be missed without this). `state:new+` ensures genuinely new models are still selected.

### Selector breakdown

| Selector | What it catches |
|----------|----------------|
| `state:modified.body+` | Models with template changes + their downstream |
| `state:modified.macros+` | Models with changed upstream macros + their downstream |
| `state:new+` | Brand new models + their downstream |
| Union of all three | Everything that matters, nothing that doesn't |

## What `state:modified` subselector options exist

From dbt docs (v1.8+):

- `state:modified.body` — SQL body changes only
- `state:modified.configs` — Config changes (excluding database/schema/alias)
- `state:modified.relation` — Database/schema/alias changes
- `state:modified.persisted_descriptions` — Description changes (if `persist_docs` enabled)
- `state:modified.macros` — Upstream macro changes
- `state:modified.contract` — Model contract changes

Plain `state:modified` includes ALL of the above plus resource-specific criteria.

## Prevention

1. **Never generate prod artifacts with `--target ci`** — the manifest locations must match where objects actually live in prod.
2. **Use `state:modified.body` + `state:modified.macros` for slim CI** when your project has target-dependent configs (warehouse sizing, masking policies, etc.) that differ between prod and CI. `.body` alone won't catch macro-only changes since it compares raw Jinja templates, not compiled SQL.
3. **Always include `state:new+`** — neither `.body` nor `.macros` catch brand new models since there's no previous state to compare against.
4. **Test CI changes against a PR with known modified models** — the workflow file is in the paths trigger, so changes to it will self-test.

## Related

- Fix PRs: #246 (initial fix), #247 (add modified.macros per Codex review)
- Original false-positive fix that introduced the bug: commit `0d9ed4e`
- Macro causing the config diff: `macros/set_warehouse_size.sql`
- dbt docs on state selectors: https://docs.getdbt.com/reference/node-selection/methods#the-state-method
