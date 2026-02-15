# Staging Model Conventions

## Overview

Staging models are the first transformation layer. They are **1:1 with source tables** — one staging model per source table. Their job is to rename columns, cast types, filter soft deletes, and present a clean contract to downstream models.

- **Materialization:** `view` (always)
- **Naming:** `stg_{source}__{table}` (double underscore separates source from table)
- **Location:** `models/operations/staging/{source}/`

## The 5-CTE Pattern

Every staging model follows this discrete CTE pipeline. Each CTE has one job. **This is enforced by `scripts/validate_staging.py`.**

```sql
{{
    config(
        materialized='view',
        tags=['{source}', 'staging', 'formentera']
    )
}}

with

-- 1. SOURCE: Raw data + deduplication if needed
source as (
    select * from {{ source('source_name', 'TABLE_NAME') }}
        -- Fivetran: deduplicate on PK by latest sync
        qualify 1 = row_number() over (partition by idrec order by _fivetran_synced desc)
        -- Estuary CDC: no dedup needed here (handled in filtered)
),

-- 2. RENAMED: Column renaming, type casting, trimming. No filtering, no logic.
renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as id_rec,
        trim(idrecparent)::varchar as id_rec_parent,

        -- descriptive fields
        trim(name)::varchar as pump_name,

        -- dates
        dttmstart::timestamp_ntz as install_date,

        -- system / audit
        trim(syscreateuser)::varchar as created_by,
        syscreatedate::timestamp_ntz as created_at_utc,

        -- ingestion metadata
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_tz as _fivetran_synced

    from source
),

-- 3. FILTERED: Remove soft deletes and null PKs. No transformations.
filtered as (
    select *
    from renamed
    where coalesce(_fivetran_deleted, false) = false
      and id_rec is not null
    -- Estuary CDC: where _operation_type != 'd'
),

-- 4. ENHANCED: Add surrogate keys, computed flags, _loaded_at. Business-light only.
enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as entity_sk,
        *,
        case
            when removal_date is null then true
            else false
        end as is_active,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        entity_sk,

        -- identifiers
        id_rec,
        id_rec_parent,

        -- descriptive fields
        pump_name,

        -- dates
        install_date,

        -- flags
        is_active,

        -- system / audit
        created_by,
        created_at_utc,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
```

## CTE Rules

| CTE | Does | Does NOT |
|-----|------|----------|
| **source** | Pulls from `{{ source() }}`, deduplicates (Fivetran: `qualify row_number()` on PK by `_fivetran_synced desc`) | Filter, rename, transform |
| **renamed** | Renames to snake_case, casts types (`trim()::varchar`, `::float`, `::timestamp_ntz`), trims strings | Filter, add logic, join |
| **filtered** | Removes soft deletes and null PKs | Transform, rename, compute |
| **enhanced** | Adds surrogate key (`dbt_utils.generate_surrogate_key`), simple computed flags, `_loaded_at` | Complex business logic (that belongs in intermediate) |
| **final** | Explicit column list grouped by category — defines the output contract | Use `SELECT *` |

## Tag Schema

All staging models use exactly 3 tags in this order:

```python
tags=['{source}', 'staging', 'formentera']
```

Examples:
- `['prodview', 'staging', 'formentera']`
- `['oda', 'staging', 'formentera']`
- `['wellview', 'staging', 'formentera']`
- `['procount', 'staging', 'formentera']`

Do not add domain-specific tags (e.g., `'gl'`, `'completions'`). Use the directory structure for domain organization.

## Column Grouping Comments

Use these comment headers to organize columns in **renamed** and **final** CTEs:

```sql
-- identifiers
-- dates
-- descriptive fields
-- measures / volumes
-- financial
-- flags
-- system / audit
-- dbt metadata
-- ingestion metadata
```

Not every group applies to every model. Use the ones that are relevant.

## Type Casting Convention

| Source Type | Cast Pattern | Example |
|------------|-------------|---------|
| String fields | `trim(col)::varchar` | `trim(idrec)::varchar as id_rec` |
| Numeric fields | `col::float` or `col::int` | `amount::float as total_amount` |
| Timestamps | `col::timestamp_ntz` | `syscreatedate::timestamp_ntz as created_at_utc` |
| Booleans | `col::boolean` | `_fivetran_deleted::boolean as _fivetran_deleted` |
| Dates | `col::date` or `{{ standardize_date('col') }}` | Use macro for null-equivalent dates |

For edge cases (empty strings, null-equivalent dates, Excel serial numbers), use the project macros:
- `{{ clean_null_string('col') }}` — `NULLIF(TRIM(col), '')`
- `{{ standardize_date('col') }}` — handles 1900-01-01, 1899-12-31
- `{{ parse_excel_date('col') }}` — converts Excel serial dates

## Metadata Columns

Prefix with underscore. Include at the end of renamed and final:

| Column | Source | Purpose |
|--------|--------|---------|
| `_fivetran_deleted` | Fivetran | Soft delete flag |
| `_fivetran_synced` | Fivetran | CDC sync timestamp |
| `_operation_type` | Estuary | CDC operation (i/u/d) |
| `_flow_published_at` | Estuary | CDC publish timestamp |
| `_loaded_at` | Enhanced CTE | `current_timestamp()` — dbt load time |

## Soft Delete Patterns by Ingestion

| Ingestion | Filter Pattern | Where |
|-----------|---------------|-------|
| Fivetran | `coalesce(_fivetran_deleted, false) = false` | `filtered` CTE |
| Estuary CDC | `_operation_type != 'd'` | `filtered` CTE |
| Portable | `deleteddate is null` | `source` CTE (pre-filter) |

## Deduplication Patterns by Ingestion

| Ingestion | Dedup Pattern | Where |
|-----------|--------------|-------|
| Fivetran | `qualify 1 = row_number() over (partition by {pk} order by _fivetran_synced desc)` | `source` CTE |
| Estuary CDC | No dedup needed (CDC guarantees one row per change) | — |
| Portable | No dedup needed | — |

## Structural Validator

The `scripts/validate_staging.py` linter enforces these conventions mechanically. Run it on every staging model change:

```bash
# Validate specific files
python scripts/validate_staging.py models/operations/staging/oda/stg_oda__gl.sql

# Validate all staging models
python scripts/validate_staging.py

# Validate only changed files
python scripts/validate_staging.py --changed

# Summary view
python scripts/validate_staging.py --format summary
```

Rules checked: `CONFIG_BLOCK`, `MATERIALIZED_VIEW`, `TAGS_*`, `CTE_MISSING_*`, `CTE_ORDER`, `SURROGATE_KEY`, `LOADED_AT`, `FINAL_EXPLICIT_COLUMNS`, `COLUMN_GROUPING`, `FINAL_SELECT`.

All errors must be fixed before committing. Warnings should be addressed when practical.
