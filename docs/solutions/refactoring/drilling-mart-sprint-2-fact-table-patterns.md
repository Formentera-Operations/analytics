---
title: "Drilling Mart Sprint 2: Fact Table Patterns for Time and NPT Events"
category: refactoring
tags: [wellview, drilling, fact-table, mart, sprint-2, materialization, data-quality-flags, clustering, null-rate-documentation]
module: operations/marts/drilling
symptoms:
  - Report date NULL when time log has no matching job report
  - Source boolean uses negative semantics (exclude_from) that confuse consumers
  - Phase FK is 71% NULL but this is expected, not a bug
  - Small fact table clustered unnecessarily adding overhead
date_solved: 2026-02-14
---

# Drilling Mart Sprint 2: Fact Table Patterns for Time and NPT Events

Sprint 2 built two drilling fact tables (`fct_drilling_time`, `fct_npt_events`) directly on top of staging models — no new intermediates needed. Six reusable patterns emerged around materialization decisions, data quality flags, and FK documentation.

## Models Built

| Model | Layer | Materialization | Grain | Rows |
|-------|-------|-----------------|-------|------|
| `fct_drilling_time` | Mart/Fact | table (clustered) | 1 row per time log entry | 762K |
| `fct_npt_events` | Mart/Fact | table | 1 row per NPT event | 14K |

## Pattern 1: Report Date Fallback with Inference Flag (Most Important)

### Problem

Time log entries reference a `job_report_id`, but 353 entries (0.05%) have no matching record in `stg_wellview__job_reports`. A bare `left join` produces NULL report dates, which breaks downstream aggregations by date.

### Bad Approach

```sql
-- WRONG: NULL report_date breaks daily aggregations
jr.report_start_datetime::date as report_date
```

### Solution

Use `coalesce()` to fall back to the activity's own start date, and add a boolean flag so consumers can distinguish actual vs inferred dates:

```sql
-- CORRECT: fallback + transparency flag
coalesce(jr.report_date, tl.start_datetime::date) as report_date,
jr.report_id is null as is_report_date_inferred
```

### Why This Matters

- **Zero NULLs** in report_date — downstream models can safely `group by report_date`
- **Transparency** — consumers can filter `where not is_report_date_inferred` for strict analysis
- **6.6% inferred** across the portfolio — a meaningful data quality metric to track over time

### Reusable Rule

When a join-derived date can be NULL, always:
1. Add a `coalesce()` fallback to the best available alternative
2. Add an `is_{column}_inferred` boolean flag
3. Document the NULL rate in the schema YAML description
4. Let consumers decide whether inferred values are acceptable for their use case

## Pattern 2: Table vs Incremental Materialization Decision

### Problem

Sprint 1 used incremental merge for `fct_daily_drilling_cost` (1.9M rows). Should Sprint 2 facts also be incremental?

### Decision Framework

| Factor | fct_daily_drilling_cost | fct_drilling_time | fct_npt_events |
|--------|------------------------|-------------------|----------------|
| Row count | 1.9M | 762K | 14K |
| Source update pattern | Append-heavy | Append-heavy | Infrequent |
| Rebuild time (table) | ~45s | ~15s | <5s |
| Incremental complexity | Worth it | Not worth it | Not worth it |

### Solution

Use `materialized='table'` for both Sprint 2 facts. The full-rebuild cost is trivial, and incremental adds:
- Merge strategy configuration
- `is_incremental()` Jinja guard
- `_loaded_at` watermark logic
- Risk of merge key collisions or missed updates
- SQLFluff indent complexity in Jinja blocks

### Reusable Rule

Use incremental when **all three** conditions are met:
1. **>1M rows** and growing (full rebuild exceeds 30s)
2. **Clear watermark column** (`_loaded_at` or CDC timestamp)
3. **Append or upsert pattern** in the source (not full-replace)

If any condition is missing, use `table` and revisit when the dataset grows.

## Pattern 3: Invert Negative-Semantics Source Booleans

### Problem

WellView stores `exclude_from_problem_time_calculations` — a negative-semantics boolean. Consumers have to think backwards:

```sql
-- CONFUSING: double negation to find countable events
where not exclude_from_problem_time_calculations
where exclude_from_problem_time_calculations = false
```

### Solution

Add a positive-semantics computed flag in the fact table:

```sql
-- In fct_npt_events enriched CTE
not coalesce(ip.exclude_from_problem_time_calculations, false) as is_countable_npt
```

Key detail: `coalesce(..., false)` treats NULL as "not excluded" (= countable), which matches business intent — if no one explicitly excluded an event, it counts.

### Reusable Rule

When a source boolean uses negative semantics (`exclude_`, `disable_`, `suppress_`):
1. Keep the original column for auditability
2. Add a positive-semantics computed flag (`is_countable_`, `is_active_`, `is_enabled_`)
3. Document the NULL handling in the YAML description
4. Consumers should use the positive flag; the original is for debugging

## Pattern 4: Document Expected NULL Rates in FK Tests

### Problem

`phase_sk` in `fct_drilling_time` is 71% NULL (537K of 762K rows). The staging source `job_program_phase_id` is legitimately NULL for time entries not tied to a specific drilling phase. A standard `relationships` test would scream about data quality.

### Solution

Use `severity: warn` on the FK relationship test and document the expected NULL rate:

```yaml
- name: phase_sk
  description: FK to dim_phase surrogate key
  data_tests:
    - relationships:
        arguments:
          to: ref('dim_phase')
          field: phase_sk
        config:
          severity: warn
```

The YAML description for `fct_drilling_time` explicitly notes "~762K rows" and the phase_sk behavior is documented in the plan.

### Reusable Rule

Not all NULL FKs are data quality issues. When a FK is legitimately NULL:
1. Use `severity: warn` (not `error`) on the `relationships` test
2. Document the expected NULL rate in the plan and/or YAML description
3. Omit `not_null` test on that FK (don't test for something you know is NULL)
4. Track the NULL rate over time — a sudden change may indicate a source issue

## Pattern 5: Cluster Only When Beneficial

### Problem

Should all fact tables be clustered? Sprint 1's `fct_daily_drilling_cost` clusters by `['well_id', 'job_id']`.

### Decision

| Model | Rows | Clustered? | Why |
|-------|------|------------|-----|
| `fct_daily_drilling_cost` | 1.9M | Yes: `['well_id', 'job_id']` | Large enough to benefit; queried by well and job |
| `fct_drilling_time` | 762K | Yes: `['job_id', 'report_date']` | Moderate size; time-series queries by job |
| `fct_npt_events` | 14K | No | Too small; full scan is instant |

### Reusable Rule

- **>500K rows**: Cluster by columns that appear in `WHERE` and `JOIN` filters
- **<100K rows**: Never cluster — micro-partition pruning can't help at this scale
- **100K–500K rows**: Cluster only if query patterns strongly favor specific columns
- **Max 2–3 cluster columns**: More columns dilute the clustering benefit

## Pattern 6: Skip the Intermediate When Enrichment Is Simple

### Problem

Sprint 1 created `int_wellview__daily_cost_enriched` as an ephemeral intermediate because cost enrichment required joining 4 sources and computing multiple derived columns. Should Sprint 2 facts also have intermediates?

### Decision

Sprint 2 facts needed only 2–3 simple lookups:

| Fact | Lookups | Computed Columns | Intermediate Needed? |
|------|---------|-----------------|---------------------|
| `fct_drilling_time` | job_reports (report date), well_360 (EID) | 2 surrogate keys, 1 coalesce, 1 flag | No |
| `fct_npt_events` | well_360 (EID) | 1 surrogate key, 1 computed flag | No |

The enrichment fits cleanly in the fact's own CTEs without compromising readability.

### Reusable Rule

Create an intermediate model when:
1. **3+ source joins** are needed for enrichment
2. **Complex business logic** (multi-step classifications, pivots, aggregations)
3. **Reuse** — the enriched dataset serves multiple downstream marts

Skip the intermediate when:
1. **1–2 simple lookups** (FK resolution, EID mapping)
2. **Inline computation** (surrogate keys, simple flags, coalesces)
3. **Single consumer** — only one mart uses this enrichment

## Validation Results

HOOEY N731H (EID 109181) ground truth matched exactly:

### fct_drilling_time

| Metric | Expected | Actual |
|--------|----------|--------|
| Time entries | 574 | 574 |
| Total hours | 2,599 | 2,599 |
| Drilling hours | 678.5 | 678.5 |

### fct_npt_events

| Metric | Expected | Actual |
|--------|----------|--------|
| NPT events | 3 | 3 |
| Gross hours | 9.0 | 9.0 |
| Category | rig equip / Surface Equipment | rig equip / Surface Equipment |

### Portfolio-Wide FK Integrity

| FK | Match Rate | Orphans | Assessment |
|----|-----------|---------|------------|
| job_sk (time) | 100% | 0 | Clean |
| job_sk (NPT) | 100% | 0 | Clean |
| phase_sk | 29% | 537K | Expected (source is 71% NULL) |
| report_date | 99.95% | 353 inferred | Handled by coalesce + flag |

## Sprint 1 vs Sprint 2 Summary

| Decision | Sprint 1 | Sprint 2 | Why Different |
|----------|----------|----------|---------------|
| Intermediate model | Yes (ephemeral) | No | Simpler enrichment |
| Materialization | Incremental merge | Table | Smaller datasets |
| Clustering | `['well_id', 'job_id']` | `['job_id', 'report_date']` / none | Query patterns differ |
| Data quality flags | None | `is_report_date_inferred`, `is_countable_npt` | New pattern established |
| FK NULL handling | All FKs populated | phase_sk 71% NULL | Documented as expected |

## Related

- [Sprint 1 Patterns](drilling-mart-sprint-1-intermediate-patterns.md) — Intermediate-to-mart patterns, inline surrogate keys, ephemeral test gotcha
- [Sprint 2 Plan](../../plans/2026-02-14-feat-sprint-2-drilling-time-npt-facts-plan.md) — Implementation plan with column contracts and validation queries
- [WellView Entity Model](../../context/sources/wellview/entity_model.md) — Physical Well and Well Work entity definitions
- [WellView 5-CTE Pattern](wellview-staging-5cte-refactor-sprint-3.md) — Staging pattern that feeds these facts
