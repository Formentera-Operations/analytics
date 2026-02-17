---
title: "Inner join after left join silently drops NULL FK rows"
category: logic-errors
tags:
  - joins
  - null-handling
  - silent-data-loss
  - oda
  - mart
module: finance
symptom: |
  dim_ap_check_register was missing ~1,095 rows (system-generated checks with NULL vendor_id). No error was raised. Row count: expected ~49,500, got 48,436.
root_cause: |
  `left join vendors` allowed NULL vendor_id through, but the subsequent `inner join entities on v.entity_id = e.id` filtered those rows back out. NULL v.entity_id can never match any e.id in an inner join, silently dropping ~2.2% of fact rows.
date: 2026-02-17
---

# Inner join after left join silently drops NULL FK rows

## Symptom

`dim_ap_check_register` produced 48,436 rows, but the source `stg_oda__apcheck` has 49,531 rows (after filtering). The ~1,095 missing rows were system-generated checks with NULL `vendor_id`. No error, no warning — the rows just disappeared.

## Root Cause: Join Chain NULL Propagation

```sql
-- Step 1: left join allows NULL vendor_id through
left join vendors as v
    on ac.vendor_id = v.id    -- NULL vendor_id → all v.* columns are NULL

-- Step 2: inner join silently filters those rows back out
inner join entities as e
    on v.entity_id = e.id     -- NULL v.entity_id never matches → row dropped
```

The problem: when a `left join` produces NULL foreign keys, any subsequent `inner join` on those NULL columns will filter the rows back out. The `left join` is effectively cancelled by the `inner join` downstream.

This is a common pattern in dimensional models where a fact table has optional relationships (e.g., vendor is NULL for system-generated records).

## Solution

Change the downstream join to `left join`:

```sql
left join vendors as v
    on ac.vendor_id = v.id
left join entities as e
    on v.entity_id = e.id     -- Changed from inner join
```

Row count: 48,436 → 49,531 (recovered ~1,095 rows).

The downstream columns (`entity_code`, `entity_name`) are now NULL for system-generated checks, which is the correct behavior — these checks genuinely have no vendor or entity.

## Detection Checklist

When reviewing mart models with multiple joins:

1. **Find all `left join` statements** — these indicate optional relationships
2. **Trace the NULL-able FK downstream** — does any subsequent `inner join` reference a column from the left-joined table?
3. **Compare row counts** — `select count(*) from fact_table` vs `select count(*) from mart_table`. If the mart has fewer rows and no WHERE clause explains it, suspect join chain NULL propagation.
4. **Check for `severity: warn` on FK tests** — if a FK has a warning-level `not_null` test (indicating expected NULLs), every downstream join on that FK must be a `left join`.

## Prevention

- **Rule of thumb**: If a FK can be NULL (evidenced by `severity: warn` on its `not_null` test), every join chain from that FK must use `left join` all the way through.
- **Review join sequences in marts** — any `inner join` following a `left join` on a related FK is a potential silent data loss bug.
- **Add row count assertions** in tests if the mart should preserve all fact rows.

## Cross-References

- `docs/solutions/refactoring/drilling-mart-sprint-2-fact-table-patterns.md` — Pattern 1: NULL handling in joins with `coalesce()` + `is_{col}_inferred` flag
- `docs/conventions/sql-patterns.md` — `IS [NOT] DISTINCT FROM` for NULL-safe comparisons
- PR #270 — Original fix in `dim_ap_check_register.sql`
