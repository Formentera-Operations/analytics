# ComboCurve

## System Overview

ComboCurve is Formentera's **reserves and economics platform**. It manages well data, production forecasts (type curves and decline curve analysis), and economic evaluations (NPV, IRR, breakeven, cash flow). It is the **system of record for reserves estimation, economic modeling, and forecast management**.

- **Vendor:** ComboCurve (SaaS)
- **API:** REST API v1 (`https://api.combocurve.com/v1/`)
- **Snowflake database:** `FO_RAW_DB`
- **Snowflake schema:** `CC_RAW`
- **Ingestion:** Portable (batch extraction via API)
- **Soft delete pattern:** `deleteddate is not null` (where applicable)
- **Deduplication:** Not required (Portable extracts are full snapshots, no CDC)
- **Ingestion timestamp:** `_PORTABLE_EXTRACTED` (mapped to `_loaded_at` in staging)
- **Table naming:** Snowflake tables are UPPER_SNAKE_CASE (e.g., `WELLS`, `PROJECT_WELLS`)

## Core Hierarchy

ComboCurve organizes data around **Projects** as the primary organizational unit. Company-level wells exist independently and are imported into projects for analysis.

```
Company Wells [WELLS]                              <- company-level well master (5K wells)
  |
Projects [PROJECTS]                                <- organizational containers (242)
  |
  +-- Project Wells [PROJECT_WELLS]                <- wells within a project (284K, many-to-many)
  |
  +-- Scenarios [PROJECT_SCENARIOS]                <- economic/forecast scenarios (956)
  |     |
  |     +-- Economic Runs [ECON_RUNS]              <- econ evaluation executions (953)
  |     |     |
  |     |     +-- One Liners [ECON_RUN_ONE_LINERS] <- summary metrics per well (1.2M)
  |     |     |
  |     |     +-- Monthly Results [ECON_RUN_MONTHLY_EXPORT_RESULTS] <- monthly cash flow (262M)
  |     |
  |     +-- (Scenario-scoped econ runs also in PROJECT_SCENARIO_ECON_RUNS)
  |
  +-- Forecasts [FORECASTS]                        <- forecast definitions (2K)
  |     |
  |     +-- Forecast Outputs [FORECAST_OUTPUTS]    <- decline curves per phase (2.1M)
  |     |
  |     +-- Daily Volumes [FORECASTED_DAILY_VOLUMES_BY_PROJECT] <- daily forecast (13K)
  |
  +-- Econ Model Options [PROJECT_ECON_MODEL_GENERAL_OPTIONS] <- model config (964)
```

### Key Relationships

| Parent | Child | Join Key | Relationship |
|--------|-------|----------|-------------|
| PROJECTS | PROJECT_WELLS | `PROJECT` | 1:many |
| PROJECTS | PROJECT_SCENARIOS | `PROJECT` | 1:many |
| PROJECTS | FORECASTS | `PROJECT` | 1:many |
| PROJECT_SCENARIOS | ECON_RUNS | `SCENARIO` + `PROJECT` | 1:many |
| ECON_RUNS | ECON_RUN_ONE_LINERS | `ECONRUN` | 1:many |
| ECON_RUNS | ECON_RUN_MONTHLY_EXPORT_RESULTS | `ECONRUN` | 1:many |
| FORECASTS | FORECAST_OUTPUTS | `FORECAST` + `PROJECT` | 1:many |
| FORECASTS | FORECASTED_DAILY_VOLUMES_BY_PROJECT | `FORECAST` + `PROJECT` | 1:many |
| WELLS | PROJECT_WELLS | `ID` -> `CHOSENID` or well match | logical |
| PROJECTS | PROJECT_ECON_MODEL_GENERAL_OPTIONS | `PROJECT` | 1:many |

### ID Patterns

- **ComboCurve IDs**: All primary keys (`ID`) are 24-character hex strings (MongoDB ObjectIds)
- **Well identifiers**: `API10`, `API12`, `API14`, `CHOSENID`, `INPTID`, `ARIESID`, `PHDWINID`
- **Cross-system key**: `CHOSENID` is the operator-chosen identifier used for matching wells across systems

## Ingestion Pattern (Portable)

| Aspect | ComboCurve (Portable) | WellView (Fivetran) |
|--------|----------------------|---------------------|
| Connector | Portable (batch API extraction) | Fivetran (CDC) |
| Soft delete | `deleteddate is not null` | `_fivetran_deleted = true` |
| Deduplication | Not needed (full snapshots) | `qualify row_number()` on `_fivetran_synced` |
| Ingestion timestamp | `_PORTABLE_EXTRACTED` | `_FIVETRAN_SYNCED` |
| `_loaded_at` mapping | `_PORTABLE_EXTRACTED` | `_FIVETRAN_SYNCED` |
| Column naming | UPPER_SNAKE_CASE (Portable converts camelCase) | UPPER_SNAKE_CASE (Fivetran preserves) |

### Portable-Specific Gotchas

1. **No CDC**: Portable does full-table extractions. No `_operation_type` or `_fivetran_deleted` columns.
2. **Column name transformation**: API uses camelCase (`firstName`), Portable converts to UPPER_SNAKE_CASE (`FIRSTNAME`) in Snowflake. Some columns lose case boundaries (e.g., `cumBoe` -> `CUMBOE`).
3. **Numeric columns stored as TEXT**: Many numeric fields in `ECON_RUN_MONTHLY_EXPORT_RESULTS` are stored as `TEXT(16777216)` despite being numeric values. Require explicit casting in staging.
4. **VARIANT columns**: Several tables use Snowflake VARIANT for nested JSON data (e.g., `FORECAST_OUTPUTS.BEST`, `ECON_RUN_ONE_LINERS.OUTPUT`, `PROJECT_ECON_MODEL_GENERAL_OPTIONS.MAINOPTIONS`).
5. **Date columns stored as TEXT**: Some date fields (e.g., `FIRSTPRODDATE`, `COMPLETIONENDDATE`, `SPUDDATE`) are stored as TEXT, not TIMESTAMP. Require date parsing in staging.
6. **`_PORTABLE_EXTRACTED`**: Available on all tables. Maps to `portableextracted` (no underscore prefix) on some tables like `ECON_RUN_ONE_LINERS`.

## Source Tables Summary

| Table | Rows | Columns | Staging Model | Domain |
|-------|------|---------|--------------|--------|
| WELLS | 5,017 | 225 | stg_cc__company_wells | wells |
| PROJECT_WELLS | 283,590 | 297 | stg_cc__project_wells | wells |
| PROJECTS | 242 | 6 | stg_cc__projects | wells |
| PROJECT_SCENARIOS | 956 | 7 | stg_cc__scenarios | economics |
| ECON_RUNS | 953 | 9 | stg_cc__economic_run_parameters | economics |
| ECON_RUN_ONE_LINERS | 1,200,692 | 12 | stg_cc__economic_one_liners | economics |
| ECON_RUN_MONTHLY_EXPORT_RESULTS | 262,344,749 | 217 | stg_cc__economic_runs | economics |
| FORECASTS | 2,043 | 10 | stg_cc__forecasts | forecasting |
| FORECAST_OUTPUTS | 2,083,923 | 23 | stg_cc__forecast_outputs | forecasting |
| FORECASTED_DAILY_VOLUMES_BY_PROJECT | 13,130 | 9 | stg_cc__daily_forecasts | forecasting |
| PROJECT_ECON_MODEL_GENERAL_OPTIONS | 964 | 17 | stg_cc__project_econ_model_general_options | economics |

### Tables Without Staging Models (Additional Raw Tables)

| Table | Rows | Notes |
|-------|------|-------|
| DAILY_PRODUCTIONS | 7,071,036 | Daily production actuals (oil, gas, water) |
| MONTHLY_PRODUCTIONS | 64,655 | Monthly production actuals |
| DIRECTIONAL_SURVEYS | 1,257 | Well directional survey data |
| PROJECT_COMPANY_WELLS | 5,015 | Company wells scoped to projects |
| PROJECT_ECON_MODEL_EXPENSES | 239,016 | Econ model expense configurations |
| PROJECT_SCENARIO_ECON_RUNS | 800 | Scenario-scoped econ runs |
| PROJECT_SCENARIO_ECON_RUN_MONTHLY_ECON_RESULTS | 3,203 | Scenario-scoped monthly econ results |
| FORECASTED_MONTHLY_VOLUMES_BY_PROJECT | 51,261 | Monthly forecast volumes |
| TYPE_CURVES | 1,255 | Type curve definitions |
| TAGS | 6 | Tag metadata |
| WELL_COMMENTS | 4 | Well comment annotations |
| USER_ROLES | 43 | User access permissions |

## Key Gotchas

1. **Staging model naming mismatch**: `stg_cc__economic_runs` reads from `ECON_RUN_MONTHLY_EXPORT_RESULTS` (262M rows), while `stg_cc__economic_run_parameters` reads from `ECON_RUNS` (953 rows). The names are counterintuitive.
2. **ECON_RUN_MONTHLY_EXPORT_RESULTS is massive**: 262M rows, 217 columns. Most columns are TEXT type despite containing numeric values. This is the largest table in the CC_RAW schema by far.
3. **Wells vs Project Wells**: `WELLS` contains company-level well data (5K, 225 cols). `PROJECT_WELLS` contains project-scoped wells with additional economic/forecast fields (284K, 297 cols). A single well can appear in multiple projects.
4. **VARIANT columns need downstream parsing**: `FORECAST_OUTPUTS` has `BEST`, `RATIO`, `TYPECURVEDATA`, `TYPECURVEAPPLYSETTINGS` as VARIANT. `ECON_RUN_ONE_LINERS.OUTPUT` is VARIANT containing all economic metrics. Keep as VARIANT in staging; parse downstream.
5. **Custom fields are tenant-specific**: `customString0-24`, `customNumber0-19`, `customBool0-4`, `customDate0-9` are configurable per ComboCurve tenant. Only a subset have data in Formentera's instance.
6. **`PROJECTCUSTOMHEADER9`**: Appears on most tables as a Portable connector artifact. Not a ComboCurve field.
7. **JSON FLATTEN in daily_forecasts**: `FORECASTED_DAILY_VOLUMES_BY_PROJECT.PHASES` is a VARIANT containing nested daily volumes by phase (oil, gas, water). Requires FLATTEN to explode into rows.
