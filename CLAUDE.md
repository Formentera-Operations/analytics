# Formentera Analytics — dbt Project

Oil & gas analytics on **dbt + Snowflake**. Two tenants: Operations (FO) and Partners (FP). ~430 models.

## Before You Write Code

1. **Load the context file** for the source you're working with → `context/sources/{source}/{source}.md` + relevant domain YAML
2. **Read 2–3 existing models** in the same layer and source to match conventions
3. **Run the validator** after every change → `python scripts/validate_staging.py <your_files>`

## Hard Rules

- **Never** run `dbt build` without `--select` — 430+ models, always scope
- **Never** hardcode database/schema — routing handled by `dbt_project.yml`
- **Never** use `SELECT *` in marts or applications — explicit columns only
- **Always** use `{{ ref() }}` and `{{ source() }}` — no hardcoded table references
- **Always** validate with `dbt show --select <model> --limit 10` before declaring done
- **Always** run `python scripts/validate_staging.py <your_files>` on staging models before committing — fix all errors

## Feedback Loop

```bash
# Single-command check (parse → lint → structural validate)
./scripts/check.sh <your_files>

# Or step by step:
python scripts/validate_staging.py <your_files>     # structural conventions
sqlfluff lint <your_files>                           # formatting
dbt build --select <model_name>                      # compilation + tests
dbt show --select <model_name> --limit 10            # data preview
```

Fix all errors from the validator before committing. Warnings are advisory but should be addressed.

## Context System

The `context/` directory is the agent's domain knowledge base. It describes the world outside dbt — source systems, business workflows, data definitions, and domain terminology.

**When to load context files:**
- **Building/modifying staging models** → load the source context (`context/sources/{source}/{source}.md` for system overview + the domain YAML for column definitions, join patterns, and unit conversions)
- **Building marts or intermediate models** → load the source context for every upstream source to understand business meaning, grain, and relationships
- **Building Cortex semantic models** → load context files for business definitions, metric formulas, and entity relationships that define how end users think about the data

**Structure:**

| Path | Contents | When to Load |
|------|----------|-------------|
| `context/sources/{source}/{source}.md` | System overview, core hierarchy, ingestion patterns, key gotchas | Always — before any work on this source |
| `context/sources/{source}/tables/{TableName}.yaml` | Column definitions for a single source table | Building/modifying a specific staging model |
| `context/sources/{source}/domains/{domain}.yaml` | Domain header, table relationships, type legend | Building intermediate/mart models that join across tables |
| `context/sources/{source}/_index.yaml` | Table catalog and domain groupings for the source | Finding which table file to load |

Per-table files are the column-level source of truth. They define what each source column means, its data type, and its business concept. Load the specific table file for the staging model you're working on — not the entire domain.

## Project Map

```
CLAUDE.md                          ← You are here (dispatch only — no deep docs)
context/
  sources/
    prodview/prodview.md           ← ProdView system overview, hierarchy, joins, conversions
    prodview/tables/*.yaml         ← 179 per-table column definition files
    prodview/domains/*.yaml        ← 12 domain relationship files
    wellview/wellview.md           ← WellView system overview, calc tables, unit conversions
    wellview/tables/*.yaml         ← 496 per-table column definition files
    wellview/domains/*.yaml        ← 16 domain relationship files
docs/
  conventions/
    staging.md                     ← 5-CTE pattern, tag schema, type casting, column grouping
    intermediate.md                ← When to create, materialization guidance
    marts.md                       ← Naming, materialization, column requirements
    sql-patterns.md                ← QUALIFY, ASOF JOIN, IS DISTINCT FROM, RANGE windows
    incremental.md                 ← Merge strategy, watermark pattern, warehouse sizing
    testing.md                     ← Test types, when to add, schema YAML organization
  reference/
    source-systems.md              ← Source catalog, CDC patterns, connection details
    macros.md                      ← Macro library with usage examples
    database-routing.md            ← Multi-tenant routing by environment
    domain-glossary.md             ← Oil & gas terminology (WI, NRI, EUR, BOE, etc.)
    packages.md                    ← dbt packages and versions
  setup/
    LOCAL_SETUP.md                 ← Dev environment, Snowflake connection, venv, gotchas
  solutions/
    build-errors/                  ← CI failure patterns and fixes
    logic-errors/                  ← Domain-specific debugging
    refactoring/                   ← Migration trackers and refactor guides
  plans/                           ← Sprint plans and implementation scripts
scripts/
  validate_staging.py              ← Structural linter (5-CTE, tags, surrogate keys, _loaded_at)
  check.sh                         ← Unified feedback loop (parse → lint → validate → build)
```

## Model Layers (Summary)

| Layer | Materialization | Naming | Key Rule |
|-------|----------------|--------|----------|
| Staging | `view` | `stg_{source}__{table}` | 5-CTE pattern, 1:1 with source → `docs/conventions/staging.md` |
| Intermediate | `ephemeral` | `int_{domain}__{description}` | Reusable transforms only → `docs/conventions/intermediate.md` |
| Marts | `table` | `fct_` / `dim_` / business names | Explicit column lists → `docs/conventions/marts.md` |
| Applications | `table` | `{app}_app__{table}` | App-specific shapes |

## Source Systems (Summary)

| Source | System | Ingestion | Soft Delete Pattern |
|--------|--------|-----------|-------------------|
| `oda` | Quorum OnDemand | Estuary CDC | `_operation_type = 'd'` |
| `prodview` | Peloton ProdView | Fivetran | `_fivetran_deleted = true` |
| `wellview` | Peloton WellView | Fivetran | `_fivetran_deleted = true` |
| `procount` | IFS Procount | Fivetran | `_fivetran_deleted = true` |
| `combo_curve` | Combo Curve | Portable | `deleteddate is not null` |
| `enverus` | Enverus | Portable | `deleteddate is not null` |

Full details: `docs/reference/source-systems.md`

## CI/CD

PRs trigger `dbt parse --warn-error` → `dbt build --select state:modified+ --defer`. Each PR gets isolated schema `DBT_CI_{PR_NUMBER}`. Think about downstream blast radius.

## Snowflake Connection (Dev)

- **Account:** `YL35090.south-central-us.azure` (NOT the org-account format)
- **Auth:** RSA keypair (`~/.snowflake/rsa_key.p8`)
- **Database:** `FO_DEV_DB` | **Role:** `DBT_ROLE`
- **Python:** 3.12 (not 3.14+ — breaks mashumaro)
- Full setup: `docs/setup/LOCAL_SETUP.md`
