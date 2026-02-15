# Multi-Tenant Database Routing

Models route to different Snowflake databases by environment. **Do not hardcode these.**

## Formentera Operations (FO)

| Layer | Prod | CI | Dev |
|-------|------|----|-----|
| Staging | `FO_STAGE_DB` | `FO_CI_DB` | `target.database` |
| Intermediate | `FO_STAGE_DB` | `FO_CI_DB` | `target.database` |
| Marts | `FO_PRODUCTION_DB` | `FO_CI_DB` | `target.database` |
| Applications | `FORMENTERA_OPS_DB` | `FO_CI_DB` | `target.database` |

## Formentera Partners (FP)

| Layer | Prod | CI | Dev |
|-------|------|----|-----|
| Staging | `FP_STAGE_DB` | `FP_CI_DB` | `target.database` |
| Intermediate | `FP_STAGE_DB` | `FP_CI_DB` | `target.database` |
| Marts | `FP_PRODUCTION_DB` | `FP_CI_DB` | `target.database` |

## Schema Routing

- **Dev/CI:** All models land in `target.schema` (e.g., `DBT_RSTOVER` or `DBT_CI_42`)
- **Prod:** Schemas are source-specific (`stg_oda`, `stg_prodview`, `marts`, `griffin`, `wiserock`)

All routing is handled by `dbt_project.yml` and the `generate_schema_name` macro. Never hardcode database or schema names in model configs.
