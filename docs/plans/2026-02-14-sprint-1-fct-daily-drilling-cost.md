# Sprint 1: `fct_daily_drilling_cost` — Implementation Plan

**Date:** 2026-02-14
**Branch:** `feature/sprint-3-wellview-staging-cleanup` (current)
**Brainstorm:** `docs/brainstorms/2026-02-14-wellview-intermediate-mart-modeling-brainstorm.md`

---

## Objective

Build the first drilling fact table: `fct_daily_drilling_cost`. This answers "What did we spend, when, on what, and is it tracking to budget?" — the highest-value question from the brainstorm.

## Models to Build

| # | Model | Layer | Materialization | Location |
|---|-------|-------|-----------------|----------|
| 1 | `int_wellview__daily_cost_enriched` | Intermediate | ephemeral | `models/operations/intermediate/drilling/` |
| 2 | `fct_daily_drilling_cost` | Mart/Fact | incremental (merge) | `models/operations/marts/drilling/` |
| 3 | Schema YAML for both models | — | — | Respective directories |

## Directory Setup

Create new directory:
```
models/operations/intermediate/drilling/
```

No new directories needed for marts (drilling/ already exists with dim_job, dim_wellbore, etc.).

---

## Task 1: `int_wellview__daily_cost_enriched` (Ephemeral Intermediate)

### Purpose
Enrich daily cost line items with report date, job context, and well_360 FK. This is a 3-model join chain (costs → reports → jobs) plus well_360 enrichment that shouldn't live in mart CTEs.

### Source Models
- `stg_wellview__daily_costs` — grain: cost line item (PK: `cost_line_id`)
- `stg_wellview__job_reports` — grain: daily report (PK: `report_id`). Join on `daily_costs.job_report_id = job_reports.report_id`
- `stg_wellview__jobs` — grain: job (PK: `job_id`). Join on `job_reports.job_id = jobs.job_id`
- `well_360` — well master. Join on `daily_costs.well_id = well_360.wellview_id`
- `dim_job` — job dimension. Join on `jobs.job_id` to get `job_sk`

### Output Columns (explicit list)

```sql
-- surrogate key
daily_cost_sk          -- from stg_wellview__daily_costs (passthrough)

-- dimensional FKs
job_sk                 -- from dim_job (MD5 of job_id)
eid                    -- from well_360 (6-char Formentera well ID)

-- natural keys
cost_line_id           -- from daily_costs
job_report_id          -- from daily_costs (FK to reports)
well_id                -- from daily_costs (WellView well GUID)
job_id                 -- from job_reports (FK to jobs)

-- report context (from job_reports)
report_date            -- job_reports.report_start_datetime::date
report_number          -- job_reports.report_number

-- job context (from jobs)
job_category           -- jobs.job_category (Drilling/Completion/Facilities/Well Servicing)
job_type_primary       -- jobs.job_type_primary

-- cost information
field_estimate_cost    -- from daily_costs
cumulative_field_estimate_cost  -- from daily_costs

-- account coding (6-level hierarchy)
main_account_id        -- from daily_costs
sub_account_id         -- from daily_costs
spend_category         -- from daily_costs
expense_type           -- from daily_costs
tangible_intangible    -- from daily_costs
afe_category           -- from daily_costs
account_name           -- from daily_costs

-- vendor
vendor_name            -- from daily_costs
vendor_code            -- from daily_costs

-- operational
ops_category           -- from daily_costs
status                 -- from daily_costs

-- purchase/work orders
purchase_order_number  -- from daily_costs
work_order_number      -- from daily_costs
ticket_number          -- from daily_costs

-- dbt metadata
_loaded_at             -- from daily_costs
```

### SQL Pattern

```sql
-- No config block needed (ephemeral is inherited from dbt_project.yml intermediate default)

with daily_costs as (
    select * from {{ ref('stg_wellview__daily_costs') }}
),

job_reports as (
    select
        report_id,
        job_id,
        report_start_datetime::date as report_date,
        report_number
    from {{ ref('stg_wellview__job_reports') }}
),

jobs as (
    select
        job_id,
        job_category,
        job_type_primary
    from {{ ref('stg_wellview__jobs') }}
),

well_360 as (
    select wellview_id, eid
    from {{ ref('well_360') }}
    where wellview_id is not null
),

dim_job as (
    select job_sk, job_id
    from {{ ref('dim_job') }}
),

enriched as (
    select
        dc.daily_cost_sk,

        -- dimensional FKs
        dj.job_sk,
        w360.eid,

        -- natural keys
        dc.cost_line_id,
        dc.job_report_id,
        dc.well_id,
        jr.job_id,

        -- report context
        jr.report_date,
        jr.report_number,

        -- job context
        j.job_category,
        j.job_type_primary,

        -- cost information
        dc.field_estimate_cost,
        dc.cumulative_field_estimate_cost,

        -- account coding
        dc.main_account_id,
        dc.sub_account_id,
        dc.spend_category,
        dc.expense_type,
        dc.tangible_intangible,
        dc.afe_category,
        dc.account_name,

        -- vendor
        dc.vendor_name,
        dc.vendor_code,

        -- operational
        dc.ops_category,
        dc.status,

        -- purchase/work orders
        dc.purchase_order_number,
        dc.work_order_number,
        dc.ticket_number,

        -- dbt metadata
        dc._loaded_at

    from daily_costs as dc
    left join job_reports as jr
        on dc.job_report_id = jr.report_id
    left join jobs as j
        on jr.job_id = j.job_id
    left join well_360 as w360
        on dc.well_id = w360.wellview_id
    left join dim_job as dj
        on jr.job_id = dj.job_id
)

select * from enriched
```

### Join Strategy
- All LEFT JOINs — cost lines should never be dropped even if report/job/well context is missing
- `daily_costs → job_reports`: on `job_report_id = report_id`
- `job_reports → jobs`: on `job_id`
- `daily_costs → well_360`: on `well_id = wellview_id`
- `job_reports → dim_job`: on `job_id` to get `job_sk`

---

## Task 2: `fct_daily_drilling_cost` (Incremental Fact)

### Purpose
Business-facing fact table at the cost line item grain. Incremental merge on `cost_line_id` with `_loaded_at` watermark.

### Config

```sql
{{
    config(
        materialized='incremental',
        unique_key='cost_line_id',
        incremental_strategy='merge',
        cluster_by=['well_id', 'job_id'],
        tags=['drilling', 'mart', 'fact']
    )
}}
```

### Source
- `int_wellview__daily_cost_enriched` — the ephemeral intermediate

### Output Columns (explicit contract)

```sql
-- surrogate key
daily_cost_sk

-- dimensional FKs
job_sk                 -- FK to dim_job
eid                    -- FK to well_360

-- natural keys
cost_line_id           -- PK (unique per cost line item)
job_report_id          -- FK to job_reports staging
well_id                -- WellView well GUID
job_id                 -- WellView job GUID

-- temporal
report_date            -- DATE: when the cost was incurred

-- job classification
job_category           -- Drilling / Completion / Facilities / Well Servicing
job_type_primary       -- Detailed job type

-- cost measures
field_estimate_cost    -- $ amount for this line item
cumulative_field_estimate_cost  -- Running cumulative cost

-- account coding (6-level hierarchy)
main_account_id
sub_account_id
spend_category
expense_type
tangible_intangible
afe_category
account_name

-- vendor
vendor_name
vendor_code

-- operational
ops_category
status

-- purchase/work orders
purchase_order_number
work_order_number
ticket_number

-- dbt metadata
_loaded_at
```

### SQL Pattern

```sql
{{
    config(
        materialized='incremental',
        unique_key='cost_line_id',
        incremental_strategy='merge',
        cluster_by=['well_id', 'job_id'],
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
    select
        daily_cost_sk,

        -- dimensional FKs
        job_sk,
        eid,

        -- natural keys
        cost_line_id,
        job_report_id,
        well_id,
        job_id,

        -- temporal
        report_date,

        -- job classification
        job_category,
        job_type_primary,

        -- cost measures
        field_estimate_cost,
        cumulative_field_estimate_cost,

        -- account coding
        main_account_id,
        sub_account_id,
        spend_category,
        expense_type,
        tangible_intangible,
        afe_category,
        account_name,

        -- vendor
        vendor_name,
        vendor_code,

        -- operational
        ops_category,
        status,

        -- purchase/work orders
        purchase_order_number,
        work_order_number,
        ticket_number,

        -- dbt metadata
        _loaded_at

    from source
)

select * from final
```

---

## Task 3: Schema YAML

### `models/operations/intermediate/drilling/schema.yml`

```yaml
version: 2

models:
  - name: int_wellview__daily_cost_enriched
    description: >
      Enriches daily cost line items with report date, job context (category, type),
      and dimensional FKs (job_sk, well EID). Ephemeral — compiles as CTE into
      fct_daily_drilling_cost.
    columns:
      - name: cost_line_id
        description: WellView cost line item natural key
        data_tests:
          - unique
          - not_null
      - name: job_sk
        description: FK to dim_job
      - name: eid
        description: FK to well_360
      - name: report_date
        description: Date the cost was incurred (from job report start datetime)
      - name: job_category
        description: "Job classification: Drilling, Completion, Facilities, or Well Servicing"
      - name: field_estimate_cost
        description: Cost amount for this line item (field estimate, base currency)
```

### Addition to `models/operations/marts/drilling/schema.yml`

```yaml
  - name: fct_daily_drilling_cost
    description: >
      Daily drilling cost fact table. One row per cost line item from WellView daily reports.
      Enriched with job classification, well EID, and report date. Incremental merge
      on cost_line_id with _loaded_at watermark. ~1.9M rows.
    columns:
      - name: daily_cost_sk
        description: Surrogate key (MD5 hash of cost_line_id)
        data_tests:
          - unique
          - not_null
      - name: cost_line_id
        description: WellView cost line item natural key
        data_tests:
          - unique
          - not_null
      - name: job_sk
        description: FK to dim_job
        data_tests:
          - relationships:
              arguments:
                to: ref('dim_job')
                field: job_sk
      - name: eid
        description: Well entity identifier — FK to well_360
        data_tests:
          - relationships:
              arguments:
                to: ref('well_360')
                field: eid
      - name: report_date
        description: Date the cost was incurred
        data_tests:
          - not_null:
              config:
                severity: warn
      - name: job_category
        description: "Job classification: Drilling, Completion, Facilities, or Well Servicing"
        data_tests:
          - accepted_values:
              values: ['Drilling', 'Completion', 'Facilities', 'Well Servicing']
              config:
                severity: warn
      - name: field_estimate_cost
        description: Cost amount for this line item (field estimate, base currency)
        data_tests:
          - not_null:
              config:
                severity: warn
```

---

## Task 4: Validation

After building, validate against HOOEY N731H ground truth:

```sql
-- Expected: ~$11.8M total across 2,123 cost lines
-- Drilling: $5,026,886 | Completion: $5,401,970 | Facilities: $1,310,729 | Well Servicing: $14,950
dbt show --select fct_daily_drilling_cost --limit 1 --inline "
  select
    job_category,
    count(*) as cost_lines,
    sum(field_estimate_cost) as total_field_estimate
  from {{ ref('fct_daily_drilling_cost') }}
  where eid = '109181'
  group by 1
  order by 2 desc
"
```

Also validate:
1. **Row count**: `dbt show` total rows should be ~1.9M
2. **PK uniqueness**: `cost_line_id` is unique (enforced by test)
3. **FK integrity**: `job_sk` and `eid` relationships pass
4. **NULL analysis**: Check NULL rates on `report_date`, `job_category`, `eid`

---

## Task 5: dbt_project.yml Update

Add intermediate/drilling to the project config if needed:

```yaml
# In models > operations > intermediate section
intermediate:
  +materialized: ephemeral
  drilling:
    +tags: ['drilling', 'intermediate']
```

---

## Execution Order

1. Create directory `models/operations/intermediate/drilling/`
2. Check/update `dbt_project.yml` for intermediate/drilling config
3. Write `int_wellview__daily_cost_enriched.sql`
4. Write `fct_daily_drilling_cost.sql`
5. Write intermediate `schema.yml`
6. Update marts `schema.yml` with fct_daily_drilling_cost
7. Run `dbt build --select +fct_daily_drilling_cost` (builds intermediate + fact + tests)
8. Validate HOOEY N731H cost totals
9. Run `sqlfluff lint` on new files
10. Run `yamllint` on new YAML files

## Risk Mitigations

- **Ephemeral debugging**: If intermediate is hard to debug, use `dbt show --select int_wellview__daily_cost_enriched --limit 10`
- **Incremental first run**: First `dbt build` does a full load (no `is_incremental()` filter). Subsequent runs are incremental.
- **Cluster by alignment**: `well_id, job_id` matches common query patterns (cost by well, cost by job)
- **NULL FKs**: LEFT JOINs preserve all cost lines. NULL `eid` or `job_sk` means the well/job didn't match — expected for orphan records.
