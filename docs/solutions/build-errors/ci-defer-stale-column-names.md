---
title: "CI --defer Fails with 'Invalid Identifier' After Column Rename in Prior PR"
category: build-errors
tags: [dbt, ci, defer, state-modified, snowflake, column-rename, stale-prod]
module: .github/workflows
symptoms:
  - "000904 (42000): SQL compilation error: invalid identifier 'COLUMN_NAME'"
  - CI fails on model not modified in the PR
  - Error references a snake_case column that exists in code but not in prod database
  - Failure only in CI, works locally
date_solved: 2026-02-12
---

# CI --defer Fails with 'Invalid Identifier' After Column Rename in Prior PR

## Problem

CI (`validate-dbt-changes`) fails with `invalid identifier 'ID_REC'` on `int_prodview__completion_downtimes`, a model **not modified** in the current PR:

```
000904 (42000): SQL compilation error: error line 11 at position 8
  invalid identifier 'ID_REC'
```

The model references `id_rec` (snake_case) from `stg_prodview__completion_downtimes`. Both the staging and intermediate models were converted to snake_case in a prior merged PR (#248), and the code on `main` is correct. The error only appears in CI.

## Root Cause

**Stale production database objects + `--defer` interaction.**

The chain of events:

1. **PR #248** merged, converting `stg_prodview__completion_downtimes` from quoted aliases (`"Id Rec"`) to snake_case (`id_rec`)
2. **Production dbt has not run** since PR #248 merged — the actual Snowflake view in prod still has the old quoted column names
3. **Current PR** modifies `int_fct_well_header`, which pulls `int_prodview__completion_downtimes` into CI via `state:modified+`
4. `int_prodview__completion_downtimes` references `stg_prodview__completion_downtimes`, which is **not modified** in this PR
5. CI's `--defer` resolves that staging ref to the **production database object** (not the code on `main`)
6. Production object still has `"Id Rec"` (quoted, case-sensitive) — `id_rec` (unquoted, resolves to `ID_REC`) doesn't match

### Why it doesn't fail locally

Local dev builds all models against the dev schema, where both staging and intermediate are freshly compiled. There's no stale object to defer to.

## Solution

**No code fix needed.** This is a transient CI issue that self-resolves:

- The next production dbt run rebuilds `stg_prodview__completion_downtimes` with snake_case columns
- Subsequent CI runs will defer to the updated prod object, and `id_rec` will resolve correctly

### If CI must pass before prod runs

Option A: **Touch the staging model** to force CI to build it (add a comment, whitespace change). This makes CI create a fresh copy in the CI schema instead of deferring to stale prod.

Option B: **Manually trigger a prod run** for the affected staging models.

## When This Happens

This pattern occurs when:

1. A PR renames columns in a staging model (e.g., quoted → snake_case)
2. That PR merges to `main`
3. Production has NOT rebuilt the staging model yet
4. A subsequent PR modifies something upstream/downstream that pulls the staging model's consumers into `state:modified+`
5. The consumer model defers the staging ref to the stale prod object

### Timeline

```
PR #248 merges (staging columns renamed in code)
     |
     |  ← prod has NOT run yet (stale objects)
     |
PR #249 opens (modifies a model upstream of the consumer)
     |
CI builds consumer via state:modified+
     |
Consumer defers staging ref → stale prod object → FAIL
     |
Prod runs → rebuilds staging view → stale object fixed
     |
CI retries → PASS
```

## Prevention

1. **Merge column-rename PRs close to scheduled prod runs** to minimize the stale window
2. **When a column-rename PR merges, note which downstream models may break in CI** until the next prod run
3. **Consider running prod immediately after merging column-rename PRs** if other PRs are pending
4. **Don't panic** — if the only CI failures are `invalid identifier` on models not in your PR, and the identifiers match recently merged renames, it's this issue

## Related

- Prior solution: `docs/solutions/build-errors/ci-defer-object-does-not-exist.md` (similar `--defer` interaction, different root cause)
- Column rename PR: #248 (refactor/prodview-staging-core-production)
- Affected model: `int_prodview__completion_downtimes` referencing `stg_prodview__completion_downtimes`
