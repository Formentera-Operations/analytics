---
title: "sqlfluff ST06 auto-fix breaks semantic comment grouping in renamed CTEs"
category: build-errors
tags:
  - sqlfluff
  - linting
  - st06
  - noqa
  - column-grouping
  - oda-staging
module: staging
symptom: |
  After running `sqlfluff fix`, comment headers in renamed CTEs no longer match the columns beneath them. Booleans (`coalesce()` expressions) end up under `-- audit` or `-- ingestion metadata` instead of `-- flags`. Timestamps end up under `-- flags` instead of `-- audit`.
root_cause: |
  sqlfluff ST06 ("Select wildcards then simple targets before calculations and aggregates") reorders the entire SELECT: simple column references first, then expressions/calculations. When auto-fixing, it moves all `coalesce()` boolean conversions to the end of the SELECT, but `--` comment headers stay in their original positions — breaking the semantic grouping defined in docs/conventions/staging.md.
date: 2026-02-17
---

# sqlfluff ST06 auto-fix breaks semantic comment grouping in renamed CTEs

## Symptom

After running `sqlfluff fix` on ODA staging models, Greptile flagged that comment headers in 4 of 5 renamed CTEs were misaligned. Example from `stg_oda__apcheck.sql`:

```sql
-- flags          ← WRONG: contains dates/timestamps
VOIDEDDATE::date as voided_date,
CREATEDATE::timestamp_ntz as created_at,
UPDATEDATE::timestamp_ntz as updated_at,

-- audit          ← WRONG: contains booleans
coalesce(RECONCILED = 1, false) as is_reconciled,
coalesce(SYSTEMGENERATED = 1, false) as is_system_generated,

-- ingestion metadata  ← WRONG: contains boolean
coalesce(VOIDED = 1, false) as is_voided
```

The `final` CTEs were correctly grouped because they only reference aliased column names (no expressions), so ST06 had nothing to reorder.

## Root Cause

sqlfluff ST06 treats the entire SELECT as one unit and enforces: wildcards → simple column references → expressions/calculations. The `coalesce(COL = 1, false)` boolean conversions are expressions, so `sqlfluff fix` moved them after all simple columns. But the `--` comment headers are invisible to sqlfluff's reordering logic — they stayed in place while columns moved past them.

This only affects the `renamed` CTE (where type casting and boolean conversion happen). The `final` CTE only has simple column references, so ST06 doesn't reorder anything.

## Solution

1. **Manually realign** comment headers in the renamed CTE to match the `final` CTE's semantic grouping
2. **Add `-- noqa: ST06`** on the `select` keyword line to prevent future auto-reordering:

```sql
renamed as (
    select -- noqa: ST06
        -- identifiers
        ID::varchar as id,
        ...

        -- flags
        coalesce(RECONCILED = 1, false) as is_reconciled,
        coalesce(SYSTEMGENERATED = 1, false) as is_system_generated,
        coalesce(VOIDED = 1, false) as is_voided,
        VOIDEDDATE::date as voided_date,
        VOID1099YEAR::int as void_1099_year,

        -- audit
        CREATEDATE::timestamp_ntz as created_at,
        UPDATEDATE::timestamp_ntz as updated_at,
        ...

        -- ingestion metadata
        FLOW_PUBLISHED_AT::timestamp_tz as _flow_published_at

    from source
),
```

## Prevention

- **Never run `sqlfluff fix` blindly** on staging models with boolean conversions — always review the diff afterward
- **Add `-- noqa: ST06`** to any renamed CTE that mixes simple columns and `coalesce()` boolean conversions in the same semantic group
- **Use the `final` CTE as the source of truth** for correct semantic grouping — if the renamed CTE's comment headers don't match the final CTE's, they're wrong
- The CC `economic_runs` model already uses `-- noqa: ST06` for the same reason (200+ columns with `try_to_decimal` casting)

## Affected Models (PR #270)

- `stg_oda__apcheck.sql` — 3 booleans
- `stg_oda__apinvoice.sql` — 6 booleans
- `stg_oda__apinvoicedetail.sql` — 3 booleans
- `stg_oda__jibdetail.sql` — 7 booleans
- `stg_oda__jib.sql` — NOT affected (no boolean columns in renamed CTE)

## Cross-References

- `docs/conventions/staging.md` — Column grouping convention (lines 138-154)
- `docs/solutions/build-errors/dbt-model-deletion-rename-procedure.md` — Existing ST06 documentation
- `scripts/validate_staging.py` — Enforces COLUMN_GROUPING structural rule (but doesn't catch misalignment within the renamed CTE)
