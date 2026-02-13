---
title: "CI Fails with 'Unknown user-defined function' on Downstream Models"
category: build-errors
tags: [dbt, ci, udf, functions, snowflake, slim-ci, well-360]
module: .github/workflows
symptoms:
  - "Unknown user-defined function FO_CI_DB.DBT_CI_xxx.NORMALIZE_WELL_STATUS"
  - "002141 (42601): SQL compilation error"
  - CI fails on downstream models that call {{ function() }} UDFs
  - Models pass locally but fail in CI
date_solved: 2026-02-12
related:
  - ci-defer-object-does-not-exist.md
---

# CI Fails with 'Unknown user-defined function' on Downstream Models

## Problem

CI (`validate-dbt-changes`) fails when building downstream models that reference Snowflake UDFs via `{{ function() }}`:

```
Failure in model int_well__prodview (models/operations/intermediate/well_360/int_well__prodview.sql)
  Database Error in model int_well__prodview
    002141 (42601): SQL compilation error:
    Unknown user-defined function FO_CI_DB.DBT_CI_248.NORMALIZE_WELL_STATUS.
```

The model compiles fine (`dbt parse` passes), and `--defer` handles unbuilt model refs correctly. But UDFs are a different resource type — `--defer` does not apply to them.

## Root Cause

Three facts interact to cause this:

### 1. UDF schema routing points to CI

The UDF config in `functions/schema.yml` routes to the CI schema:

```yaml
config:
  database: "{{ {'prod': 'FO_STAGE_DB', 'ci': 'FO_CI_DB'}.get(target.name, target.database) }}"
  schema: "{{ target.schema if target.name in ['ci', 'dev'] else 'udfs' }}"
```

In CI, `{{ function('normalize_well_status') }}` resolves to `FO_CI_DB.DBT_CI_248.NORMALIZE_WELL_STATUS`.

### 2. UDFs are not included in `state:modified+`

The CI selector `state:modified.body+ state:modified.macros+ state:new+` only picks up **modified** or **new** resources. The UDFs themselves weren't changed in this PR — they've existed since commit `c62ad49`. So they're excluded from the build.

### 3. Downstream models ARE included

Modifying ProdView staging models (the actual PR work) causes downstream models like `int_well__prodview` to be selected via the `+` graph operator. When that model compiles, it references the UDF at the CI schema location — which was never created.

### Why this didn't fail before

The `well_360` models and UDFs were added after the slim CI workflow was set up. Previous CI runs never hit a downstream model that called `{{ function() }}` because those models weren't in the modified graph.

## Solution

Added a "Deploy UDFs" step in `.github/workflows/validate-dbt-changes.yaml` **before** the main `dbt build`:

```yaml
- name: Deploy UDFs
  run: >
    dbt run
    --select "resource_type:function"
    --profile integration_tests
    --target ci
```

This creates all 4 UDFs in the CI schema before any model tries to reference them:

- `normalize_well_status`
- `normalize_state_abbrev`
- `normalize_well_config`
- `normalize_producing_method`

Also added `functions/**` to the workflow's path triggers so UDF changes themselves trigger CI.

### Why not add to the build selector?

Adding `resource_type:function` to the `dbt build --select` line would also work, but a separate step is better because:

1. **Explicit in CI logs** — easy to see if UDF deployment itself fails vs. a model failure
2. **Guaranteed ordering** — UDFs exist before any model compilation, regardless of DAG resolution
3. **Low cost** — 4 deterministic UDFs deploy in seconds

## Investigation Steps

1. Read the CI error: `Unknown user-defined function FO_CI_DB.DBT_CI_248.NORMALIZE_WELL_STATUS`
2. Found the reference in `int_well__prodview.sql` line 66: `{{ function('normalize_well_status') }}(status)`
3. Checked `functions/schema.yml` — confirmed CI routes to `FO_CI_DB.<target.schema>`
4. Checked CI selector — confirmed `state:modified+` wouldn't include unchanged UDFs
5. Verified `dbt ls --select "resource_type:function"` lists all 4 UDFs locally
6. Added deploy step and pushed

## Prevention

1. **Any CI workflow that uses slim builds (`state:modified+`) must deploy UDFs separately** — `--defer` does not resolve UDF references to production.
2. **When adding new UDFs**, verify CI passes by checking if any downstream model in the modified graph calls `{{ function() }}`.
3. **When adding new `{{ function() }}` calls to models**, ensure the UDF deploy step exists in CI.

## Related

- Fix commit: `96642d6` on branch `refactor/prodview-staging-core-production`
- Previous CI defer fix: [ci-defer-object-does-not-exist.md](ci-defer-object-does-not-exist.md) (PRs #246, #247)
- UDFs added: commit `c62ad49` (dbt 1.11 UDFs for well_360 normalization)
- UDF definitions: `functions/schema.yml`, `functions/normalize_*.sql`
- Affected model: `models/operations/intermediate/well_360/int_well__prodview.sql`
