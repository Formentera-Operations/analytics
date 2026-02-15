# Portable Column Naming Mismatches (ComboCurve)

**Date:** 2026-02-15
**Category:** Ingestion
**Source:** ComboCurve (Portable connector)
**Severity:** Medium — causes incorrect documentation, not data errors
**Status:** To raise with Portable team

## Problem

Portable's ComboCurve connector transforms API field names (camelCase) to Snowflake column names (UPPER_SNAKE_CASE), but the transformation is **not a predictable case conversion**. Some field names are structurally renamed, making it impossible to derive Snowflake column names from the API spec alone.

## Affected Tables

### ECON_RUN_MONTHLY_EXPORT_RESULTS (48 columns)

The entire CAPEX breakdown section uses a different naming structure in Snowflake than the API.

| API Field (camelCase) | Expected Snowflake | Actual Snowflake |
|-----------------------|-------------------|-----------------|
| `drillingCapex` | `DRILLINGCAPEX` | `TOTALDRILLING` |
| `drillingIntangibleCapex` | `DRILLINGINTANGIBLECAPEX` | `INTANGIBLEDRILLING` |
| `drillingTangibleCapex` | `DRILLINGTANGIBLECAPEX` | `TANGIBLEDRILLING` |

This pattern repeats across 16 CAPEX categories: abandonment, appraisal, artificiallift, completion, development, drilling, exploration, facilities, leasehold, legal, otherinvestment, pad, pipelines, salvage, waterline, workover.

**Total: 48 columns (16 categories x 3 types)**

### PROJECT_WELLS (22+ columns)

Economic and reserves fields use different names than the API.

| API Field | Expected Snowflake | Actual Snowflake |
|-----------|-------------------|-----------------|
| `internalRateOfReturn` | `INTERNALRATEOFRETURN` | `IRR` |
| `payoutMonths` | `PAYOUTMONTHS` | `PAYOUTDURATION` |
| `undiscountedRoiBefit` | `UNDISCOUNTEDROIBEFIT` | `UNDISCOUNTEDROI` |
| `investmentPerWell` | `INVESTMENTPERWELL` | *(does not exist)* |
| `eurOil` | `EUROIL` | *(does not exist — use OILSHRUNKEUR)* |
| `eurGas` | `EURGAS` | *(does not exist — use GASSHRUNKEUR)* |
| `remainOilReserves` | `REMAINOILRESERVES` | *(does not exist)* |

Working interest/NRI fields for non-oil phases (gas, NGL, drip) exist in the API but not in Snowflake.

## Impact

- **Documentation accuracy**: Context files and column catalogs built from API docs will have wrong column names
- **No data impact**: The Snowflake columns contain correct data regardless of naming
- **Developer confusion**: Engineers referencing API docs to build dbt models will use wrong column names

## Workaround

Always validate column names against Snowflake `information_schema.columns`, never rely solely on API documentation:

```sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'CC_RAW'
  AND table_name = 'ECON_RUN_MONTHLY_EXPORT_RESULTS'
ORDER BY ordinal_position;
```

## Ask for Portable Team

1. **Publish a column mapping file** — a CSV or JSON mapping API field names to Snowflake column names per table, so downstream consumers can programmatically reconcile
2. **Document the transformation rules** — explain why some fields get structural renames (e.g., `drillingCapex` → `TOTALDRILLING`) rather than simple case conversion
3. **Flag renamed fields in connector docs** — at minimum, annotate fields where the Snowflake name differs from the API name beyond case conversion
