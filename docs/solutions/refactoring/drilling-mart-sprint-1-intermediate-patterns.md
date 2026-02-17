---
title: "Drilling Mart Sprint 1: Intermediate-to-Mart Patterns and Gotchas"
category: refactoring
tags: [wellview, drilling, intermediate, ephemeral, surrogate-key, incremental, mart, sprint-1, DAG-design, data-tests]
module: operations/intermediate/drilling, operations/marts/drilling
symptoms:
  - Intermediate model unnecessarily joining to mart dimension for surrogate key
  - dbt tests silently skipped on ephemeral models
  - accepted_values test too restrictive for evolving source enumerations
  - SQLFluff indent errors inside Jinja conditional blocks
date_solved: 2026-02-14
---

# Drilling Mart Sprint 1: Intermediate-to-Mart Patterns and Gotchas

Sprint 1 built the first drilling fact table (`fct_daily_drilling_cost`) with an ephemeral intermediate (`int_wellview__daily_cost_enriched`). Five reusable patterns emerged during code review and validation.

## Models Built

| Model | Layer | Materialization | Grain |
|-------|-------|-----------------|-------|
| `int_wellview__daily_cost_enriched` | Intermediate | ephemeral | 1 row per cost line item |
| `fct_daily_drilling_cost` | Mart/Fact | incremental (merge) | 1 row per cost line item |

## Pattern 1: Compute Surrogate Keys Inline (Most Important)

### Problem

The original plan had the ephemeral intermediate joining to `dim_job` (a materialized mart) to fetch `job_sk`:

```sql
-- WRONG: creates intermediate → mart DAG dependency
dim_job as (
    select job_sk, job_id from {{ ref('dim_job') }}
),

enriched as (
    select
        dj.job_sk,  -- fetched from mart
        ...
    from daily_costs as dc
    left join dim_job as dj on jr.job_id = dj.job_id
)
```

This creates unnecessary DAG coupling. When the intermediate is ephemeral, it compiles as a CTE inside the mart — so the mart would reference itself indirectly.

### Solution

Compute the surrogate key inline using `generate_surrogate_key()`. The function is deterministic (MD5 hash), so it produces identical values:

```sql
-- CORRECT: self-contained, no mart dependency
enriched as (
    select
        {{ dbt_utils.generate_surrogate_key(['jr.job_id']) }} as job_sk,
        ...
    from daily_costs as dc
    left join job_reports as jr on dc.job_report_id = jr.report_id
)
```

### Reusable Rule

Any intermediate enriching staging data with dimensional FKs should:
1. Compute surrogate keys inline via `{{ dbt_utils.generate_surrogate_key([...]) }}`
2. Never join to dimension marts to fetch pre-computed surrogate keys
3. Document the derivation: "Surrogate key (MD5 hash of {natural_key})"

This keeps the DAG clean: `staging → intermediate → mart`, never `staging → intermediate → mart ↔ mart`.

## Pattern 2: Ephemeral Models Silently Skip Tests

### Problem

Added `unique` and `not_null` tests to the ephemeral intermediate's `schema.yml`:

```yaml
# WRONG: tests are silently skipped on ephemeral models
models:
  - name: int_wellview__daily_cost_enriched
    columns:
      - name: cost_line_id
        data_tests:
          - unique      # silently skipped
          - not_null     # silently skipped
```

Ephemeral models don't create Snowflake objects — dbt's test runner has nothing to query. Tests are skipped with **no warning or error**, creating a false sense of coverage.

### Solution

Remove all `data_tests` from ephemeral model YAML. Add equivalent tests to the downstream materialized model:

```yaml
# CORRECT: tests on the materialized fact table
models:
  - name: fct_daily_drilling_cost  # table/incremental — tests execute
    columns:
      - name: cost_line_id
        data_tests:
          - unique
          - not_null
```

### Reusable Rule

- Ephemeral models: document columns in YAML (descriptions only), no `data_tests`
- All test assertions go on the first downstream materialized model
- Debug ephemeral logic with `dbt show --select int_model_name --limit 10`

## Pattern 3: Don't Use accepted_values for Source Enumerations

### Problem

Added `accepted_values` for `job_category` with 4 values from HOOEY N731H ground truth:

```yaml
# WRONG: too restrictive for evolving source data
- name: job_category
  data_tests:
    - accepted_values:
        arguments:
          values: ['Drilling', 'Completion', 'Facilities', 'Well Servicing']
```

The portfolio has many more valid categories (LWO, Capital WO, WORKOVER, etc.). The test warned on 61 unexpected values — all legitimate.

### Solution

Replace with `not_null` at warn severity:

```yaml
# CORRECT: validates presence without restricting values
- name: job_category
  data_tests:
    - not_null:
        config:
          severity: warn
```

### Reusable Rule

- Use `accepted_values` only for columns with a **fixed, closed enumeration** (e.g., boolean flags, regulatory categories)
- For source-driven classifications that may evolve, use `not_null` with `severity: warn`
- If strict enumeration is needed, maintain values in a seed table and test with `relationships`

## Pattern 4: Incremental Merge Template

The fact table established the project's incremental merge pattern for large fact tables:

```sql
{{
    config(
        materialized='incremental',
        unique_key='cost_line_id',       -- natural key for merge
        incremental_strategy='merge',     -- Snowflake merge
        on_schema_change='sync_all_columns',
        cluster_by=['well_id', 'job_id'], -- match query patterns
        tags=['drilling', 'mart', 'fact']
    )
}}

with source as (
    select * from {{ ref('int_wellview__daily_cost_enriched') }}
    {% if is_incremental() %}
        where _loaded_at > (select max(_loaded_at) from {{ this }})
    {% endif %}
),

final as (
    select ... from source
)

select * from final
```

Key decisions:
- **`unique_key`**: Use the natural key, not the surrogate key (deterministic merge behavior)
- **`cluster_by`**: Choose columns that match common query patterns (by well, by job)
- **`on_schema_change='sync_all_columns'`**: Safely handle column additions without manual DDL
- **Watermark**: `_loaded_at` (set in staging via `current_timestamp()`)
- **SQLFluff**: Indent `where` inside `{% if is_incremental() %}` to 8 spaces

## Pattern 5: SQLFluff Indent in Jinja Blocks

SQLFluff requires nested indentation inside Jinja conditional blocks:

```sql
-- WRONG: 4-space indent inside Jinja block
{% if is_incremental() %}
    where _loaded_at > (select max(_loaded_at) from {{ this }})
{% endif %}

-- CORRECT: 8-space indent (nested inside the CTE + Jinja block)
    {% if is_incremental() %}
        where _loaded_at > (select max(_loaded_at) from {{ this }})
    {% endif %}
```

Pre-commit hooks catch this automatically. Run `sqlfluff lint models/file.sql` when writing Jinja-heavy models.

## Validation Results

HOOEY N731H ground truth matched exactly:

| Job Category | Cost Lines | Total Field Estimate |
|-------------|-----------|---------------------|
| Drilling | 1,210 | $5,026,886 |
| Completion | 763 | $5,401,970 |
| Facilities | 141 | $1,310,729 |
| Well Servicing | 9 | $14,950 |

Portfolio total: 1,901,701 rows across 5,229 wells, $8.2B field estimate.

## Related

- [Sprint 2 Patterns](drilling-mart-sprint-2-fact-table-patterns.md) — Fact table patterns: materialization decisions, data quality flags, FK NULL documentation
- [WellView Entity Model](../../context/sources/wellview/entity_model.md) — Physical Well and Well Work entity definitions
- [Sprint 1 Plan](../../docs/plans/2026-02-14-sprint-1-fct-daily-drilling-cost.md) — Implementation plan with column contracts
- [dbt YAML Test Config](../build-errors/ci-dbt-parse-missing-arguments-deprecation.md) — `config:` wrapper requirement for test severity
- [WellView 5-CTE Sprint 3](wellview-staging-5cte-refactor-sprint-3.md) — Staging pattern that feeds this intermediate
- [ProdView 5-CTE Pattern](prodview-staging-5-cte-pattern.md) — Original 5-CTE pattern reference
