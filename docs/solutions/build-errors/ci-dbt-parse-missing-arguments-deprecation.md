---
title: "CI Fails on dbt parse --warn-error with MissingArgumentsPropertyInGenericTestDeprecation"
category: build-errors
tags: [dbt, ci, warn-error, deprecation, yaml, generic-tests, partial-parse]
module: .github/workflows
symptoms:
  - "Compilation Error in models/.../_.yml"
  - "MissingArgumentsPropertyInGenericTestDeprecation"
  - "Found top-level arguments to test 'not_null' defined on 'model_name'"
  - Passes locally but fails in CI
  - Error only in full parse (--no-partial-parse), not partial parse
date_solved: 2026-02-13
---

# CI Fails on dbt parse --warn-error with MissingArgumentsPropertyInGenericTestDeprecation

## Problem

CI (`validate-dbt-changes`) fails at the `dbt parse --warn-error` step with:

```
Compilation Error in models/operations/staging/wellview/general/_stg_wellview.yml
  Invalid generic test configuration given in
  models/operations/staging/wellview/general/_stg_wellview.yml:

  [WARNING][MissingArgumentsPropertyInGenericTestDeprecation]:
  Found top-level arguments to test 'not_null' defined on
  'stg_wellview__well_header': {'severity': 'warn'}
```

The YAML compiles and runs fine locally with `dbt parse` and `dbt build`.

## Root Cause

**Partial parse cache masks deprecation warnings; CI does full parse.**

In dbt 1.11, generic tests with dictionary-form configuration require the `config:` wrapper for non-standard properties like `severity`. Placing `severity` directly under the test name is deprecated.

```yaml
# WRONG — bare severity (deprecated, triggers warning)
data_tests:
  - not_null:
      severity: warn

# CORRECT — severity nested under config:
data_tests:
  - not_null:
      config:
        severity: warn
```

**Why it passes locally but fails in CI:**

1. **Local development** uses partial parse (`target/partial_parse.msgpack`). If the model was already cached before the YAML was added, the deprecation warning may not surface.
2. **CI** runs `dbt parse --warn-error` with a fresh environment (no partial parse cache). The full parse encounters the deprecation and emits a warning, which `--warn-error` promotes to a compilation error.

Running `dbt parse --warn-error --no-partial-parse` locally reproduces the CI failure.

## Solution

Wrap `severity: warn` under `config:` in the YAML:

```yaml
# Before (broken in CI)
columns:
  - name: eid
    data_tests:
      - not_null:
          severity: warn

# After (works everywhere)
columns:
  - name: eid
    data_tests:
      - not_null:
          config:
            severity: warn
```

Applied to 2 occurrences in `general/_stg_wellview.yml` (columns `eid` and `spud_date` on `stg_wellview__well_header`).

## Prevention

1. **Always use `config:` wrapper** for test configuration properties (`severity`, `where`, `limit`, `store_failures`, `schema`). The bare form is deprecated in dbt 1.11 and will be removed in a future version.

2. **Verify locally with full parse** before pushing schema YAML changes:
   ```bash
   dbt parse --warn-error --no-partial-parse
   ```

3. **Pattern to follow** for all schema YAML test configs:
   ```yaml
   data_tests:
     - not_null:
         config:
           severity: warn
     - relationships:
         to: ref('other_model')
         field: id
         config:
           severity: warn
   ```

4. **Also watch for `tests:` vs `data_tests:`** — the `tests:` key is deprecated in dbt 1.5+. Always use `data_tests:` in new YAML.

## Related

- `docs/solutions/build-errors/ci-defer-stale-column-names.md` — Another CI-only failure pattern (stale prod objects)
- `models/operations/staging/wellview/general/_stg_wellview.yml` — File that was fixed
- dbt docs: [Generic test configuration](https://docs.getdbt.com/reference/resource-properties/data-tests#generic-test-configuration)
