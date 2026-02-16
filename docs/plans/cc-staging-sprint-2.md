# Sprint 2: ComboCurve Staging 5-CTE Refactor

**Branch:** `feature/cc-staging-5cte-refactor`
**Scope:** Refactor 11 CC staging models to 5-CTE pattern + YAML column docs
**Status:** In Progress

## Work Packages

### WP1: Wells Domain (3 models)
| Model | Source Table | Rows | Complexity |
|-------|-------------|------|-----------|
| stg_cc__company_wells | WELLS | 5K | Simple — rename + cast |
| stg_cc__project_wells | PROJECT_WELLS | 284K | Simple — rename + cast |
| stg_cc__projects | PROJECTS | 242 | Simple — 4 columns |

**Files:** 3 SQL refactors + `_stg_cc__wells.yml` (new)

### WP2: Forecasting Domain (3 models)
| Model | Source Table | Rows | Complexity |
|-------|-------------|------|-----------|
| stg_cc__forecasts | FORECASTS | 2K | Simple — rename + cast |
| stg_cc__forecast_outputs | FORECAST_OUTPUTS | 2.1M | Medium — VARIANT cols, remove business filter |
| stg_cc__daily_forecasts | FORECASTED_DAILY_VOLUMES_BY_PROJECT | 13K→553M | Complex — multi-level FLATTEN |

**Files:** 3 SQL refactors + `_stg_cc__forecasting.yml` (new)

### WP3: Economics Domain (5 models)
| Model | Source Table | Rows | Complexity |
|-------|-------------|------|-----------|
| stg_cc__scenarios | PROJECT_SCENARIOS | 956 | Simple — 5 columns |
| stg_cc__economic_run_parameters | ECON_RUNS | 953 | Simple — 6 columns |
| stg_cc__economic_runs | ECON_RUN_MONTHLY_EXPORT_RESULTS | 262M | Large — 200+ TEXT→numeric cols |
| stg_cc__economic_one_liners | ECON_RUN_ONE_LINERS | 1.2M | Medium — JSON extraction + macros |
| stg_cc__project_econ_model_general_options | PROJECT_ECON_MODEL_GENERAL_OPTIONS | 964 | Medium — VARIANT parsing |

**Files:** 5 SQL refactors + `_stg_cc__economics.yml` (new, absorbs existing one_liners.yml)

## 5-CTE Template (Portable Variant)

### Key Differences from WellView (Fivetran)
1. **No dedup in source** — Portable does full snapshots, not CDC
2. **Soft delete in source CTE** — `where deleteddate is null` (where column exists)
3. **`_portable_extracted`** replaces `_fivetran_synced` / `_fivetran_deleted`
4. **`_loaded_at`** = `current_timestamp()` per staging convention (keep `_portable_extracted` as separate metadata column)
5. **Tags** = `['combo_curve', 'staging', 'formentera']` (3 canonical tags)

### Standard Template
```sql
{{
    config(
        materialized='view',
        tags=['combo_curve', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('combo_curve', 'TABLE_NAME') }}
),

renamed as (
    select
        -- identifiers
        trim(id)::varchar as model_id,

        -- descriptive fields
        ...

        -- dates
        createdat::timestamp_ntz as created_at,
        updatedat::timestamp_ntz as updated_at,

        -- ingestion metadata
        _portable_extracted::timestamp_tz as _portable_extracted

    from source
),

filtered as (
    select *
    from renamed
    where model_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['model_id']) }} as model_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        model_sk,
        -- identifiers
        model_id,
        -- ... grouped columns ...
        -- dbt metadata
        _portable_extracted,
        _loaded_at
    from enhanced
)

select * from final
```

## Model-Specific Notes

### stg_cc__company_wells / stg_cc__project_wells
- Nearly identical schemas (~30 columns selected from 225/297 available)
- `regexp_replace(county, ...)` stays in renamed (string cleanup, not logic)
- `case when customstring3 = 'OP'` operator flag moves to enhanced
- Typo fix: `operator_cateogry` → `operator_category`

### stg_cc__daily_forecasts
- Multi-level FLATTEN requires extra CTEs between filtered and enhanced
- Structure: source → renamed → filtered → phases_parsed → series_extracted → daily_volumes → enhanced → final
- Keep `ref('corporate_reserve_scenarios')` filter in source CTE
- Remove `where volume is not null` from final (move to filtered or downstream)

### stg_cc__economic_one_liners
- JSON `get(output, 'key')::float` extraction moves to enhanced CTE
- `transform_company_name()` and `transform_reserve_category()` macros stay in enhanced
- Keep `output` VARIANT column in renamed for pass-through
- Surrogate key on composite: `['economic_one_liner_id']` (not the 5-column composite)

### stg_cc__economic_runs (262M rows)
- All 200+ columns need `try_to_decimal(COL, 38, 6)` casting in renamed
- Remove `ORDER BY` from final
- Surrogate key on `id` (the `_ID` column), not the 5-column composite
- `economic_run_well_id` composite key moves to enhanced

### stg_cc__forecast_outputs
- Remove `WHERE is_forecasted = true` from final (business filter belongs downstream)
- Remove `ORDER BY` from final
- Computed columns (is_approved, forecast_date, forecast_month) move to enhanced
- Keep VARIANT columns (best, ratio, typecurvedata) as-is in renamed

### stg_cc__project_econ_model_general_options
- VARIANT parsing (boeconversion, discounttable, incometax, mainoptions, reportingunits) stays in renamed
- This is type casting/extraction, not business logic

## YAML Documentation Pattern

Each domain YAML follows the WellView pattern:
- Model description: source table, grain, key transformations
- Column groups with `# -- group` headers
- Description format: `"Business meaning. ComboCurve: SOURCE_COLUMN"`
- Tests: `unique` + `not_null` on natural key, `not_null` on FKs
- Use `config: severity: warn` wrapper (not bare severity)

## Validation Checklist

After all models are refactored:
1. `python scripts/validate_staging.py models/operations/staging/combo_curve/` — zero errors
2. `dbt parse --warn-error --no-partial-parse` — zero errors
3. `sqlfluff lint models/operations/staging/combo_curve/` — clean
4. `dbt build --select tag:combo_curve+tag:staging` — builds successfully
5. Delete `stg_cc__economic_one_liners.yml` (absorbed into `_stg_cc__economics.yml`)
