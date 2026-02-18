---
title: "fix: dim_accounts Daly Waters G&A category mapping + defensive los_category test"
type: fix
date: 2026-02-18
---

# fix: dim_accounts Daly Waters G&A Category Mapping + Defensive dbt Test

## Enhancement Summary

**Deepened on:** 2026-02-18
**Agents used:** data-reviewer, performance-oracle, code-simplicity-reviewer, data-integrity-guardian

### Key Improvements Added

1. **Critical: `int_accounts_classified.sql` conflict** — this model maps `'DALY WATERS'` to `'Lease Operating Expenses'` while the fix proposes `'G&A'`. Requires business confirmation before any code change.
2. **Critical: fct_los full-refresh is mandatory**, not optional — the incremental watermark never re-processes historical rows whose dimension values changed; all 627 existing NULL rows persist until explicit `--full-refresh`.
3. **Syntax fix: WHERE clause** — remove `los_report_header != 'ACCRUAL'` filter; `'ACCRUAL'` is not a value that appears in `dim_accounts`'s CASE (it's an `int_accounts_classified` concept).
4. **New test: `los_category_line_number`** — the secondary sort CASE also has no `else` clause; a defensive test on it is warranted.
5. **fct_los test scoping** — the `expression_is_true` test on `fct_los` must include a `_loaded_at >= '2026-02-18'` filter or it will fail on all pre-fix historical rows.
6. **Architecture note** — the `los_category` derivation belongs in `stg_sharepoint__los_account_map`, not `dim_accounts`; this is a tracked follow-up, not blocking.

---

## Overview

627 rows ($3.1M) from Daly Waters Energy accounts are dropping into NULL `los_category` in `fct_los` because `'DALY WATERS'` is absent from the G&A branch of the `los_category` CASE statement in `dim_accounts`. This is a data quality fix plus a defensive test to prevent silent regression on future LOS MAPPING MASTER Excel updates.

## Problem Statement

The `los_category` CASE statement in `dim_accounts.sql` (lines 166–204) has no `else` clause. Any `los_report_header` value not explicitly enumerated returns NULL — silently. Accounting added `DALY WATERS` as a G&A sub-section to the LOS MAPPING MASTER Excel (line 82, between CORP INSURANCE and HARDWR & SOFTWR), but the CASE statement was never updated.

**Three affected accounts:**
| Account | Sub-Account | Description |
|---------|-------------|-------------|
| 930 | 43 | DWE DIRECT COSTS |
| 930 | 976 | ACCRUED DALY WATERS |
| 930 | 977 | DALY WATERS G&A ALLOC TRUE UP |

**Impact:** 627 rows, ~$3.1M G&A spend incorrectly showing `los_category = NULL` in `fct_los` semantic model.

### ⚠️ Pre-work Required: Confirm Category Classification

`int_accounts_classified.sql` maps `'DALY WATERS'` to `'Lease Operating Expenses'`, while this plan proposes `'G&A'`. These two models must agree. **Confirm with Finance which classification is correct before making any code changes.** Both models will be updated to the agreed value.

## Proposed Solution

### Pre-condition: Confirm correct category with Finance

Query to check current state and identify any other potential conflicts:
```sql
-- What does int_accounts_classified say about DALY WATERS?
SELECT los_report_header, los_category
FROM {{ ref('int_accounts_classified') }}
WHERE los_report_header = 'DALY WATERS';
-- Currently returns: los_category = 'Lease Operating Expenses'

-- What does dim_accounts currently return?
SELECT los_report_header, los_category
FROM {{ ref('dim_accounts') }}
WHERE los_report_header = 'DALY WATERS';
-- Currently returns: los_category = NULL (missing from CASE)
```

### Change 1: Add `'DALY WATERS'` to the correct category branch

**File:** `models/operations/marts/finance/dim_accounts.sql` (lines ~166–204)

Assuming Finance confirms G&A (per the LOS MAPPING MASTER Excel):

```sql
-- BEFORE
when lm.los_report_header in (
    'CMPNY PR & BNFT', 'CNSL & CNTR EMP', 'HARDWR & SOFTWR', 'OFFICE RENT',
    'CORP FEES', 'CORP INSURANCE', 'AUDIT', 'LEGAL', 'REAL PROP TAX',
    'TRAVEL', 'UTIL & INTERNET', 'VEHICLES', 'SUPPLIES & EQP', 'MISCELLANEOUS'
) then 'G&A'
```

```sql
-- AFTER
-- G&A: standard overhead categories + Daly Waters Energy allocations (LOS MAPPING MASTER line 82)
when lm.los_report_header in (
    'CMPNY PR & BNFT', 'CNSL & CNTR EMP', 'HARDWR & SOFTWR', 'OFFICE RENT',
    'CORP FEES', 'CORP INSURANCE', 'AUDIT', 'LEGAL', 'REAL PROP TAX',
    'TRAVEL', 'UTIL & INTERNET', 'VEHICLES', 'SUPPLIES & EQP', 'MISCELLANEOUS',
    'DALY WATERS'  -- Daly Waters Energy direct costs and G&A allocations (LOS MAPPING MASTER line 82)
) then 'G&A'
else null  -- intentionally unmapped: los_report_header not classified (check stg_sharepoint__los_account_map)
end as los_category,
```

**Note on `los_category_line_number`:** No separate fix needed. The secondary CASE (lines 281–294) already maps `'G&A' → 12`. Once DALY WATERS accounts get `los_category = 'G&A'` from the primary fix, the line number resolves automatically.

### Change 2: Update `int_accounts_classified.sql` to match

**File:** `models/operations/intermediate/finance/int_accounts_classified.sql` (or wherever it lives)

Update to use the same agreed category. This model has zero active dbt downstream consumers but may be queried directly via BI tools. Both models must agree.

### Change 3: Add defensive tests to `schema.yml`

**File:** `models/operations/marts/finance/schema.yml`

The schema.yml is currently sparse (11 lines). Add model-level and column-level tests:

```yaml
models:
  - name: dim_accounts
    description: "Dimension table mapping GL accounts to LOS categories and report hierarchy."
    data_tests:
      # Catch any future los_report_header values not in the los_category CASE (severity: warn to not block CI)
      - dbt_utils.expression_is_true:
          expression: "los_category is not null"
          config:
            where: "is_los_account = true and los_report_header is not null"
            severity: warn
      # Guard against los_category_line_number diverging from los_category (this CAN be error — it's structural)
      - dbt_utils.expression_is_true:
          expression: "los_category_line_number is not null"
          config:
            where: "los_category is not null"
            severity: error

  - name: fct_los
    description: "Incremental fact table of LOS GL transactions by account, category, and period."
    data_tests:
      # Monitor for new NULL los_category rows AFTER the fix date (pre-fix historical rows are expected NULL until full-refresh)
      - dbt_utils.expression_is_true:
          expression: "los_category is not null"
          config:
            where: "los_section is not null and los_section != '' and _loaded_at >= '2026-02-18'"
            severity: warn
```

**YAML syntax notes:**
- `expression_is_true` takes `expression` directly (NOT under `arguments:` — unlike `unique_combination_of_columns`)
- `where` and `severity` must be inside `config:` — bare `severity:` at the test level fails `--warn-error`
- Always verify with `dbt parse --warn-error --no-partial-parse` before committing

## Technical Considerations

### Why `fct_los` full-refresh is MANDATORY (not optional)

`fct_los` is incremental with watermark:
```sql
{% if is_incremental() %}
    AND _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
{% endif %}
```

The 627 affected GL transactions were written to the fact table in previous runs with `los_category = NULL`. Their `_loaded_at` timestamps predate the current watermark. **After `dim_accounts` is rebuilt, `fct_los` incremental runs will NOT re-touch these rows.** They remain permanently `NULL` without an explicit `--full-refresh`.

This is a documented pattern — see `int_gl_enhanced.sql` line 29: "Late-arriving dimension updates require periodic full refresh."

**`dbt run --select fct_los --full-refresh` is a required post-deploy step, not a consideration.**

### Impact on `fct_los` clustering

`fct_los` clusters on `['company_code', 'journal_date', 'los_category', 'los_section']`. The 627 pre-fix rows sit in NULL micro-partitions for `los_category`. Without a full-refresh, queries filtering `WHERE los_category = 'G&A'` still scan the null-bucket partitions for historical DALY WATERS data, degrading pruning efficiency.

After a `--full-refresh`, the table rebuilds with correct values uniformly distributed, restoring cluster efficiency. Verify with:
```sql
SELECT SYSTEM$CLUSTERING_INFORMATION('fct_los');
```

### `dbt_utils.expression_is_true` vs. `not_null` column test

Both approaches are valid. `not_null` on a column entry is slightly simpler:
```yaml
columns:
  - name: los_category
    data_tests:
      - not_null:
          config:
            where: "is_los_account = true"
            severity: warn
```

However, `expression_is_true` is used here because it better communicates the intent (this is a business mapping completeness check, not a key constraint) and allows the expression to be self-documenting. Either form passes `--warn-error` with the `config:` wrapper.

### `fct_eng_GL` impact

`fct_eng_GL.sql` does a `RIGHT JOIN` to `dim_accounts` filtered to `is_los_account = TRUE`. It does not select `los_category` — it uses dim_accounts only for account filtering. After the fix, DALY WATERS accounts remain `is_los_account = TRUE`; their presence in the join result is unchanged. No action required; `fct_eng_GL` is a `table` materialization and rebuilds fully on next run.

### Architecture improvement opportunity (follow-up)

The `los_category` derivation logic belongs in `stg_sharepoint__los_account_map` — that staging model already has `los_report_header` and all the classification context. Moving the CASE there would:
- Remove 39 lines from `dim_accounts.sql`
- Centralize all category mapping in one place
- Make future additions (like the next DALY WATERS) a one-file change

This is **not blocking** — ship the one-line fix now, track the refactor separately.

## Rollout Sequence

```
1. Confirm category with Finance (G&A or LOE?)
2. Update dim_accounts.sql + int_accounts_classified.sql to agreed category
3. Add expression_is_true tests to schema.yml
4. Run: dbt parse --warn-error --no-partial-parse  (verify YAML)
5. Run: dbt build --select dim_accounts --no-partial-parse  (verify fix)
6. Run: dbt show --select dim_accounts --limit 5 (spot-check DALY WATERS rows)
7. Run: dbt run --select fct_los --full-refresh  (heal historical rows — MANDATORY)
8. Run: dbt test --select dim_accounts fct_los  (verify tests pass)
9. Run: dbt run --select fct_eng_GL  (table rebuild, no special flags needed)
```

**Do NOT** run `dbt build --select state:modified+` as the primary deployment — it will run `fct_los` incrementally and leave the 627 historical rows as NULL. Always use the explicit `--full-refresh` as step 7.

## Acceptance Criteria

- [ ] Finance confirms `'DALY WATERS'` belongs in G&A (or LOE) category
- [ ] `dim_accounts` G&A branch includes `'DALY WATERS'` with block comment citing LOS MAPPING MASTER line 82
- [ ] `int_accounts_classified` updated to same category as `dim_accounts`
- [ ] `else null -- intentionally unmapped` comment added to `los_category` CASE
- [ ] Two `expression_is_true` tests on `dim_accounts` (los_category, los_category_line_number)
- [ ] One `expression_is_true` test on `fct_los` with `_loaded_at >= '2026-02-18'` scope
- [ ] `dbt parse --warn-error --no-partial-parse` passes
- [ ] `dbt build --select dim_accounts` passes with 0 errors
- [ ] `dbt run --select fct_los --full-refresh` completes successfully
- [ ] Verification queries below confirm 0 NULL los_category rows for DALY WATERS

## Verification Queries

```sql
-- 1. Verify DALY WATERS accounts are now categorized in dim_accounts
SELECT los_report_header, los_category, los_category_line_number, COUNT(*) as account_count
FROM {{ ref('dim_accounts') }}
WHERE los_report_header = 'DALY WATERS'
GROUP BY 1, 2, 3;
-- Expected: 3 rows, all with los_category = 'G&A', los_category_line_number = 12

-- 2. Verify DALY WATERS rows now have category in fct_los
SELECT los_section, los_category, COUNT(*) as row_count
FROM {{ ref('fct_los') }}
WHERE los_section = 'DALY WATERS'
GROUP BY 1, 2;
-- Expected: 1 row, los_category = 'G&A', row_count ~= 627

-- 3. Verify no other orphaned sections remain
SELECT los_section, COUNT(*) as row_count
FROM {{ ref('fct_los') }}
WHERE los_category IS NULL
  AND los_section IS NOT NULL
  AND los_section != ''
GROUP BY 1
ORDER BY 2 DESC;
-- Expected: 0 rows

-- 4. Confirm int_accounts_classified agrees with dim_accounts
SELECT
    d.los_report_header,
    d.los_category as dim_category,
    c.los_category as int_category
FROM {{ ref('dim_accounts') }} d
JOIN {{ ref('int_accounts_classified') }} c USING (los_report_header)
WHERE d.los_category != c.los_category;
-- Expected: 0 rows (no divergence between models)
```

## Files to Change

```
models/operations/marts/finance/
  ├── dim_accounts.sql          ← Add 'DALY WATERS' to G&A IN list, add else null comment (~line 200)
  └── schema.yml                ← Add expression_is_true tests for dim_accounts + fct_los

models/operations/intermediate/finance/  (or wherever int_accounts_classified lives)
  └── int_accounts_classified.sql  ← Update DALY WATERS to match agreed category
```

## Dependencies & Risks

**Pre-condition:** Finance team must confirm G&A vs. LOE before any code change.

**Risk:** `expression_is_true` test syntax might vary; verify with `dbt parse --warn-error` before committing. The `where` key belongs inside `config:`, not as a sibling to `expression:`.

**Risk:** CI will run a full build of `fct_los` on this PR (since `is_incremental()` is false in the CI schema). Budget for M-warehouse CI cost.

**Risk:** If `--full-refresh` on `fct_los` is skipped or deferred, the `expression_is_true` test on fct_los will fire as a warning on every run until the refresh is done. This is acceptable (it's severity: warn) but should be tracked.

## Follow-up Issues to Track

1. **Architecture refactor:** Move `los_category` CASE from `dim_accounts.sql` into `stg_sharepoint__los_account_map`. Eliminates the mapping CASE from the mart layer entirely.
2. **`los_category_line_number` in `dim_accounts`**: Also has no `else` clause. Add a defensive test now (included in this PR), track the architecture refactor separately.
3. **Periodic full-refresh documentation**: Add a note to `fct_los.sql` config block that dimension changes to `dim_accounts` (specifically `los_category`, `los_section`, `los_key_sort`) require `--full-refresh` of `fct_los`.

## References

- `dim_accounts.sql:166-204` — `los_category` CASE statement (G&A branch at ~line 193)
- `dim_accounts.sql:281-294` — `los_category_line_number` sort order (self-corrects with the fix)
- `fct_los.sql:7` — cluster key includes `los_category`
- `int_accounts_classified.sql` — conflicting DALY WATERS → LOE mapping (needs reconciliation)
- `docs/conventions/testing.md` — test YAML syntax requirements
- `docs/solutions/build-errors/ci-dbt-parse-missing-arguments-deprecation.md` — `where` belongs in `config:` not bare
- `docs/solutions/logic-errors/inner-join-after-left-join-drops-null-rows.md` — NULL propagation pattern
- `docs/solutions/refactoring/drilling-mart-sprint-1-intermediate-patterns.md` — accepted_values scope guidance
