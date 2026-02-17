# Incremental Model Pattern

When a model needs incremental materialization, follow this pattern:

```sql
{{
    config(
        materialized='incremental',
        unique_key='primary_key_column',
        incremental_strategy='merge',
        cluster_by=['partition_col_1', 'partition_col_2']
    )
}}

with source_data as (
    select * from {{ ref('upstream_model') }}
    {% if is_incremental() %}
    where _loaded_at > (select max(_loaded_at) from {{ this }})
    {% endif %}
)

select ... from source_data
```

## Key Rules

| Setting | Value | Why |
|---------|-------|-----|
| **Strategy** | Always `merge` | Snowflake-native, handles inserts and updates |
| **Watermark column** | `_loaded_at` | Set in staging enhanced CTE via `current_timestamp()` |
| **Cluster by** | Columns matching common query patterns | Company, date, account — improves query performance |
| **Large models** | `{{ config(snowflake_warehouse=set_warehouse_size('M')) }}` | Use medium warehouse for compute-heavy incrementals |

## When to Use Incremental

- The source data is large enough that full rebuilds are slow or expensive
- There's a reliable watermark column (`_loaded_at`, `_fivetran_synced`, `updated_at`)
- The model is referenced frequently and needs fast refresh

Most staging and intermediate models should **not** be incremental. Reserve it for large fact tables in marts and specific intermediate models like `int_gl_enhanced`.
