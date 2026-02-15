# Marts & Applications Conventions

## Marts (`models/operations/marts/`)

Marts are **business-facing, query-ready** models. They are the primary interface for analysts, BI tools, and applications.

- **Materialization:** `table` (or `incremental` for large fact tables)
- **Naming:** `fct_{grain}` for facts, `dim_{entity}` for dimensions, or business-friendly names (e.g., `general_ledger`, `economics`)
- **Location:** `models/operations/marts/{domain}/`
- **Subdomains:** `finance/`, `production/`, `griffin/` (Barnett-specific), `drilling/`

### Key Rules

- **Explicit column lists required** — no `SELECT *` in marts
- **No hardcoded database/schema** — routing handled by `dbt_project.yml`
- **Document with schema YAML** — every mart should have model-level description and column descriptions for key fields

## Applications (`models/operations/applications/`)

Application models are shaped specifically for a downstream application. They exist because the application needs data in a specific format that doesn't match the general-purpose mart structure.

- **Materialization:** `table`
- **Naming:** `{app_name}_app__{table}` (e.g., `wiserock_app__daily_forecasts`)
- **Location:** `models/operations/applications/{app_name}/`

### Current Applications

| App | Description | Models |
|-----|-------------|--------|
| Wiserock | Well analytics platform | 67 models in `applications/wiserock/` |
