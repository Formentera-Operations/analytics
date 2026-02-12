# Formentera Analytics — dbt Project

## Project Overview

Oil & gas analytics platform built on **dbt + Snowflake**. Two tenants: Formentera Operations (FO, primary) and Formentera Partners (FP, early stage). ~430 models across staging, intermediate, marts, and application layers.

**Warehouse:** Snowflake | **IDE:** Paradime | **CI:** GitHub Actions | **Observability:** Elementary

## Hard Rules

- **Never run `dbt build` without `--select`** — 430+ models, always scope to what you changed
- **Never hardcode database or schema names** — routing is handled by `dbt_project.yml` and `generate_schema_name` macro
- **Never use `SELECT *` in marts or applications** — explicit column lists only in production-facing models
- **Always use `{{ ref() }}` and `{{ source() }}`** — never hardcode table references
- **Always validate with `dbt show`** before declaring a model complete — check row counts, NULLs, and grain
- **CI runs `state:modified+`** — your changes will be built along with all downstream models; think about blast radius

## Source Systems

| Source | System | Domain | Key Tables |
|--------|--------|--------|------------|
| `oda` | Quorum OnDemand Accounting | Finance (GL, AP, AR, AFEs, JIB, revenue/expense decks) | `GL`, `ODA_BATCH_ODA_*` |
| `prodview` | Peloton ProdView | Production volumes, allocations, completions | `FORMENTERAOPS_PV30_DBO.*` |
| `wellview` | Peloton WellView | Well master data, drilling, surveys, costs | Multiple schemas |
| `procount` | IFS Procount | Barnett Shale (Griffin acquisition) production | `FP_GRIFFIN.PUBLIC.*` |
| `combo_curve` | Combo Curve | Economics forecasting (EUR, NPV, type curves) | Economic runs, wells, projects |
| `enverus` | Enverus | Third-party well/production data | `PUBLIC.*` |
| `aegis` | Aegis | Market pricing data (commodities) | 6 tables |
| `hubspot` | HubSpot | CRM contacts | 1 table |

**CDC pattern:** ODA tables arrive via Estuary CDC. Soft deletes are indicated by `"_meta/op" = 'd'` — filter these out in staging. The `FLOW_PUBLISHED_AT` column tracks CDC timestamps.

## Model Layers & Conventions

### Staging (`models/operations/staging/`)
- **Materialized as:** `view`
- **Naming:** `stg_{source}__{table}` (double underscore separates source from table)
- **Purpose:** 1:1 with source tables. Rename columns to snake_case, cast types, apply NULL handling, filter soft deletes
- **CTE pipeline:** Every staging model follows this discrete CTE pattern. Each CTE has one job:

```sql
{{
    config(
        materialized='view',
        tags=['source_name', 'staging', 'formentera']
    )
}}

with

-- 1. SOURCE: Raw data + deduplication if needed
source as (
    select * from {{ source('source_name', 'TABLE_NAME') }}
        -- Fivetran sources: deduplicate on PK by latest sync
        qualify 1 = row_number() over (partition by idrec order by _fivetran_synced desc)
        -- Estuary CDC sources: no dedup needed here (handled in filtered)
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

-- 4. ENHANCED: Add surrogate keys, computed flags, _loaded_at. Business-light derivations only.
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

**CTE rules:**
- **source** — Raw pull + deduplication. Fivetran: `qualify row_number()` on PK by `_fivetran_synced desc`. Estuary CDC: no dedup needed.
- **renamed** — Only renaming, casting, and trimming. No filtering, no logic, no joins.
- **filtered** — Remove soft deletes and null primary keys. Fivetran: `coalesce(_fivetran_deleted, false) = false`. Estuary: `_operation_type != 'd'`.
- **enhanced** — Surrogate keys (`dbt_utils.generate_surrogate_key`), simple computed flags, and `_loaded_at` timestamp. Keep it light — complex logic belongs in intermediate.
- **final** — Explicit column list grouped by logical category. This defines the model's output contract.

**Column grouping comments:** Use `-- identifiers`, `-- dates`, `-- descriptive fields`, `-- flags`, `-- system / audit`, `-- dbt metadata`, etc.

**Metadata columns:** Prefix with underscore (`_fivetran_synced`, `_fivetran_deleted`, `_loaded_at`, `_operation_type`, `_flow_published_at`).

**Type casting convention:** Use `trim(col)::varchar`, `col::float`, `col::timestamp_ntz` inline in the renamed CTE. Use project macros (`{{ standardize_date() }}`, `{{ clean_null_string() }}`) for edge-case handling.

### Intermediate (`models/operations/intermediate/`)
- **Materialized as:** `ephemeral` (inherited from `dbt_project.yml` — do NOT override to `view` unless you have a specific reason)
- **Naming:** `int_{domain}__{description}` (e.g., `int_gl_enhanced`, `int_griffin__daily_production`)
- **Purpose:** Transform staging data into shapes ready for marts. An intermediate model exists to isolate a _reusable_ transformation that would otherwise be duplicated or create an unreadably complex mart.
- **Note:** Many existing intermediate models override to `view` — this is legacy, not intentional. New models should use the ephemeral default. Use `dbt show` to preview/debug ephemeral models.

**When to create an intermediate model:**
1. **Multi-source join** — Combining 2+ staging models into one enriched dataset (e.g., `int_prodview__production_volumes` joins allocations + downtimes + parameters + status)
2. **Entity spine** — Creating an authoritative list of entities across source systems (e.g., `int_well__spine` unifies well EIDs from ODA, ProdView, WellView, Combo Curve)
3. **Business classification** — Applying domain rules that multiple marts need (e.g., `int_accounts_classified` maps account codes to LOS categories, interest types, expense classifications)
4. **Aggregation or reshape** — Changing grain or pivoting data before it reaches marts (e.g., `int_oda_ar_invoice_payments_agg`)
5. **Source enrichment** — Adding dimensional context to a source before mart consumption (e.g., `int_griffin__completions_enriched`)

**When NOT to create an intermediate model:**
- The transformation is only used by one mart — put it in CTEs within that mart instead
- You're just renaming or filtering — that belongs in staging
- You're building a final business entity (dimension or fact) — that's a mart

**Materialization guidance:**
- **`ephemeral`** (default) — Use for most intermediate models. Compiles as a CTE into downstream models, no Snowflake object created. Debug with `dbt show --select model_name`
- **`table`** — Override when the model has expensive logic (complex CASE chains, many joins) AND is referenced by 3+ downstream models (e.g., `int_accounts_classified`)
- **`incremental`** — Override only for very large datasets that can't be fully rebuilt each run (e.g., `int_gl_enhanced`, `int_general_ledger_enhanced` which use merge strategy on `_loaded_at`)
- Only add an explicit `config(materialized=...)` block when overriding the default — ephemeral models need no config block

### Marts (`models/operations/marts/`)
- **Materialized as:** `table` (or `incremental` for large fact tables)
- **Naming:** `fct_{grain}` for facts, `dim_{entity}` for dimensions, or business-friendly names (e.g., `general_ledger`, `economics`)
- **Purpose:** Business-facing, query-ready. Explicit column lists required
- **Subdomains:** `finance/`, `production/`, `griffin/` (Barnett-specific)

### Applications (`models/operations/applications/`)
- **Materialized as:** `table`
- **Naming:** `{app_name}_app__{table}` (e.g., `wiserock_app__daily_forecasts`)
- **Purpose:** Shaped specifically for a downstream application (Wiserock well analytics)

## Multi-Tenant Database Routing

Models route to different Snowflake databases by environment. **Do not hardcode these.**

| Layer | Prod (FO) | CI | Dev |
|-------|-----------|----|-----|
| Staging | `FO_STAGE_DB` | `FO_CI_DB` | `target.database` |
| Intermediate | `FO_STAGE_DB` | `FO_CI_DB` | `target.database` |
| Marts | `FO_PRODUCTION_DB` | `FO_CI_DB` | `target.database` |
| Applications | `FORMENTERA_OPS_DB` | `FO_CI_DB` | `target.database` |

**Schema routing:** In dev/CI, all models land in `target.schema` (e.g., `DBT_RSTOVER` or `DBT_CI_42`). In prod, schemas are source-specific (`stg_oda`, `stg_prodview`, `marts`, `griffin`, `wiserock`).

## Macro Library

Use these instead of writing inline SQL. They handle edge cases already.

| Macro | Purpose | Example |
|-------|---------|---------|
| `clean_null_string(col)` | `NULLIF(TRIM(col), '')` — converts empty strings to NULL | `{{ clean_null_string('description') }}` |
| `clean_null_int(col)` | `NULLIF(col, 0)` — converts 0 to NULL (Procount FK convention) | `{{ clean_null_int('gatheringsystemid') }}` |
| `standardize_date(col)` | Handles NULL-equivalent dates (`1900-01-01`, `1899-12-31`) and casts to DATE | `{{ standardize_date('effectivedate') }}` |
| `standardize_timestamp(date_col, time_col)` | Combines separate date/time columns into TIMESTAMP | `{{ standardize_timestamp('userdatestamp', 'usertimestamp') }}` |
| `parse_excel_date(col)` | Converts Excel serial date numbers to DATE (handles leap year bug) | `{{ parse_excel_date('startdate') }}` |
| `is_date_effective(start, end)` | Returns BOOLEAN for date-range validity checks | `{{ is_date_effective('startdate', 'enddate') }}` |
| `generate_surrogate_key(fields)` | MD5 hash of concatenated fields | `{{ generate_surrogate_key(['merrickid', 'type']) }}` |
| `set_warehouse_size(size)` | Routes to env-appropriate warehouse (XS/S/M) | `{{ config(snowflake_warehouse=set_warehouse_size('M')) }}` |
| `transform_company_name(col)` | Standardizes company name variations | `{{ transform_company_name('company_name') }}` |
| `transform_reserve_category(col)` | Maps reserve classification codes | `{{ transform_reserve_category('reserve_cat') }}` |

**Procount-specific macros** in `macros/procount_helpers/`: `decode_object_type`, `generate_object_key`, `object_type_case`.

**ProdView-specific macro** in `macros/prodview_helpers/`: `prodview_unit_conversions`.

## Incremental Model Pattern

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

- **Strategy:** Always `merge` (Snowflake)
- **Watermark column:** `_loaded_at` (set in staging via `CURRENT_TIMESTAMP()`)
- **Cluster by:** Choose columns that match common query patterns (company, date, account)
- **Large models:** Use `{{ config(snowflake_warehouse=set_warehouse_size('M')) }}` for compute-heavy incrementals

## Testing Patterns

### Existing test types in use
- **Generic tests:** `unique`, `not_null`, `relationships`, `accepted_values` (in schema YAML)
- **dbt_expectations:** `expect_column_values_to_be_between`, `expect_column_value_lengths_to_be_between`
- **Elementary:** `volume_anomalies`, `freshness_anomalies`, `column_anomalies` (for data observability)

### When adding tests to a model
1. Primary key: always add `unique` + `not_null`
2. Foreign keys: add `relationships` to the referenced model
3. Enumerated columns: add `accepted_values`
4. Financial columns on critical models: add Elementary `column_anomalies` (sum, zero_count)
5. High-volume models: add Elementary `volume_anomalies` with appropriate time bucket

### Schema YAML organization
- One YAML file per directory or logical source grouping
- Source definitions: `src_{source_name}.yml` in the staging directory for that source
- Model definitions: `schema.yml` or `models.yml` colocated in the model directory

## Packages

| Package | Version | Use For |
|---------|---------|---------|
| `dbt_utils` | 1.3.2 | `surrogate_key`, `star`, test utilities |
| `dbt_expectations` | 0.10.10 | Advanced data quality tests |
| `elementary` | 0.21.0 | Data observability, anomaly detection |
| `dbt_snow_mask` | 0.2.7 | Column-level masking policies (prod only) |

Do not add packages without discussing with the team first.

## CI/CD Pipeline

**Trigger:** PRs that modify `models/`, `macros/`, `seeds/`, `snapshots/`, `dbt_project.yml`, or `packages.yml`.

**What it does:**
1. `dbt parse --warn-error` — validates project syntax
2. Checks out `main` and parses to generate prod artifacts for state comparison
3. `dbt build --select state:modified+ --defer --state prod-artifacts/` — builds only changed models + downstream, referencing prod for unchanged upstream models

**Key implications:**
- Your model must parse cleanly with `--warn-error`
- Downstream models from your change will also be built and tested
- Each PR gets an isolated schema: `DBT_CI_{PR_NUMBER}`

## Oil & Gas Domain Glossary

Common terms that appear in column names and business logic:

| Term | Meaning |
|------|---------|
| WI | Working Interest — company's share of costs |
| NRI | Net Revenue Interest — company's share of revenue |
| EUR | Estimated Ultimate Recovery — total expected production |
| NPV | Net Present Value — discounted cash flow |
| LOE | Lease Operating Expenses |
| LOS | Lease Operating Statement |
| AFE | Authorization for Expenditure — capital approval |
| JIB | Joint Interest Billing — cost sharing between partners |
| GL | General Ledger |
| Deck | Revenue/expense ownership allocation schedule |
| BOE | Barrel of Oil Equivalent (6:1 gas-to-oil conversion) |
| BOPD/MCFD | Barrels Oil Per Day / Thousand Cubic Feet per Day |
| Griffin | Internal name for Barnett Shale acquisition (via Procount) |

## Verification Commands

```bash
# After modifying models — build only what you changed + downstream
dbt build --select state:modified+

# Quick check — compile without running
dbt compile --select model_name

# Preview data — always do this before declaring done
dbt show --select model_name --limit 10

# Run tests only
dbt test --select model_name

# Check lineage before modifying a model
# Use the dbt MCP server: get_model_lineage_dev(model_id="model_name", direction="children", recursive=true)
```

## File Organization

```
analytics/
├── models/
│   ├── operations/          # Formentera Operations (primary tenant)
│   │   ├── staging/         # 239 models — stg_{source}__{table}
│   │   │   ├── oda/         # Accounting (42 models)
│   │   │   ├── prodview/    # Production (67 models)
│   │   │   ├── wellview/    # Wells (75 models)
│   │   │   ├── procount/    # Griffin/Barnett (30 models)
│   │   │   ├── combo_curve/ # Economics (11 models)
│   │   │   └── ...
│   │   ├── intermediate/    # 55 models — int_{domain}__{description}
│   │   │   ├── finance/
│   │   │   ├── production/
│   │   │   ├── griffin/
│   │   │   └── well_360/
│   │   ├── marts/           # 66 models — fct_/dim_/business names
│   │   │   ├── finance/
│   │   │   ├── production/
│   │   │   └── griffin/
│   │   └── applications/    # 67 models — {app}_app__{table}
│   │       └── wiserock/
│   └── partners/            # Formentera Partners (3 placeholder models)
├── macros/                  # 16 custom macros
├── seeds/                   # 4 CSV reference files
├── tests/                   # Singular tests
└── snapshots/               # (empty — future SCD tracking)
```
