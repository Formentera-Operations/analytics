# Combo Curve Staging Cleanup

**Date:** 2026-02-15
**Status:** Ready for planning
**Scope:** 11 staging models + context documentation

## What We're Building

Two-sprint effort to bring Combo Curve staging into alignment with the WellView standard:

1. **Sprint 1 (Context Documentation):** Build the `context/sources/combo_curve/` documentation tree from API docs + actual Snowflake data
2. **Sprint 2 (5-CTE Refactor):** Refactor all 11 staging models to the 5-CTE pattern and add comprehensive YAML column docs (using context files from Sprint 1)

### Models in Scope (Sprint 2)

1. `stg_cc__company_wells`
2. `stg_cc__daily_forecasts` (JSON FLATTEN — complex)
3. `stg_cc__economic_one_liners` (JSON extraction — complex)
4. `stg_cc__economic_run_parameters`
5. `stg_cc__economic_runs`
6. `stg_cc__forecast_outputs` (JSON variant columns)
7. `stg_cc__forecasts`
8. `stg_cc__project_econ_model_general_options`
9. `stg_cc__project_wells`
10. `stg_cc__projects`
11. `stg_cc__scenarios`

## Why This Approach

- **Consistency**: WellView set the standard (62/62 models, 2,000+ columns documented). Combo Curve at 0% compliance is the obvious next target.
- **Context first**: WellView taught us that having context files before refactoring dramatically speeds up YAML doc writing. Build the knowledge base first.
- **Manageable scope**: 11 models fits cleanly in a single sprint and PR for the refactor.
- **Ingestion differences are minor**: Portable uses `deleteddate is null` instead of Fivetran's `_fivetran_deleted`/dedup pattern — straightforward substitution in source CTE.

## Key Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Sprint order | Context docs first, then refactor | Context files speed up YAML column doc writing |
| Context doc source | API docs + Snowflake cross-reference | Handles Portable-to-Snowflake naming gaps; most accurate |
| Context structure | Full WellView pattern | Consistent across all sources (overview + index + tables/ + domains/) |
| `daily_forecasts` handling | Same treatment as other 10 | FLATTEN logic fits in enhanced CTE; keep as view |
| Documentation depth | Full column docs, every model | Models are small; consistency with WellView standard |
| `economic_one_liners.yml` | Regenerate from scratch | Ensures template consistency across all 11 models |
| PR strategy | Single PR per sprint | Sprint 1 = context docs PR; Sprint 2 = refactor PR |

---

## Sprint 1: Context Documentation

### Data Sources

- **ComboCurve API docs:** https://docs.api.combocurve.com/
- **OpenAPI spec:** https://storage.googleapis.com/beta-combocurve-api-docs/openapi-spec.yaml
- **Snowflake `information_schema`**: Cross-reference actual column names and types

### API Resource Hierarchy

```
Company Wells (/v1/wells)                     → stg_cc__company_wells
Projects (/v1/projects)                        → stg_cc__projects
├── Project Wells                              → stg_cc__project_wells
├── Scenarios                                  → stg_cc__scenarios
│   └── Econ Runs                              → stg_cc__economic_runs
│       ├── One Liners                         → stg_cc__economic_one_liners
│       └── Monthly Results                    → stg_cc__economic_run_parameters
├── Forecasts                                  → stg_cc__forecasts
│   └── Forecast Outputs                       → stg_cc__forecast_outputs
│       └── Daily Volumes                      → stg_cc__daily_forecasts
└── Econ Model General Options                 → stg_cc__project_econ_model_general_options
```

### Deliverables

```
context/sources/combo_curve/
├── combo_curve.md              ← System overview, hierarchy, ingestion pattern, key gotchas
├── _index.yaml                 ← Table catalog and domain groupings
├── tables/
│   ├── wells.yaml              ← ~200+ columns (wells endpoint)
│   ├── project_wells.yaml      ← Same schema as wells (project-scoped)
│   ├── projects.yaml
│   ├── project_scenarios.yaml
│   ├── forecasts.yaml          ← (project-scoped)
│   ├── forecast_outputs.yaml   ← Decline curve segments, P10/P50/P90
│   ├── forecasted_daily_volumes_by_project.yaml  ← JSON phases
│   ├── econ_runs.yaml
│   ├── econ_run_one_liners.yaml          ← JSON output variant
│   ├── econ_run_monthly_export_results.yaml  ← ~200+ financial columns
│   └── project_econ_model_general_options.yaml  ← VARIANT config fields
└── domains/
    ├── wells.yaml              ← Company wells + project wells relationships
    ├── forecasting.yaml        ← Forecasts → outputs → daily volumes
    └── economics.yaml          ← Scenarios → econ runs → one liners / monthly
```

### Approach

1. Download OpenAPI spec for structured field definitions
2. Query Snowflake `information_schema.columns` for each source table to get actual column names/types
3. Cross-reference API field names (camelCase) with Snowflake column names (Portable transforms to UPPER_SNAKE_CASE or preserves case)
4. Build per-table YAML files with: column name, data type, description, business meaning, units where applicable
5. Write system overview markdown with hierarchy, join patterns, ingestion specifics
6. Create domain groupings (wells, forecasting, economics)

### Key API Endpoints Documented

| Resource | API Endpoint | Fields | Notes |
|----------|-------------|--------|-------|
| Wells | `GET /v1/wells` | ~200+ | Identifiers, geometry, production metrics, completion, gas analysis, custom fields |
| Forecasts | `GET /v1/projects/{id}/forecasts` | ~10 | id, name, type, runDate, tags |
| Forecast Outputs | `GET /v1/projects/{id}/forecasts/{id}/outputs` | ~20+ | Decline curve segments (best/P10/P50/P90), EUR, phase, ratio |
| Econ Runs | `GET /v1/projects/{id}/scenarios/{id}/econ-runs` | ~10 | id, runDate, status, tags, outputParams |
| Projects | `GET /v1/projects` | ~5 | id, name, createdAt, updatedAt |

---

## Sprint 2: 5-CTE Refactor + YAML Docs

### Ingestion Pattern (Portable vs Fivetran)

| Aspect | Combo Curve (Portable) | WellView (Fivetran) |
|--------|----------------------|---------------------|
| Soft delete | `deleteddate is null` | `_fivetran_deleted = false` |
| Deduplication | Not needed | `qualify row_number()` on `_fivetran_synced` |
| Ingestion timestamp | `_portable_extracted` | `_fivetran_synced` |
| `_loaded_at` mapping | `_portable_extracted` | `_fivetran_synced` |

### Known Complexity

- **`stg_cc__daily_forecasts`**: Expands ~101K source rows to ~553M via nested JSON FLATTEN. FLATTEN logic moves to enhanced CTE.
- **`stg_cc__economic_one_liners`**: Extracts metrics from JSON `output` column. JSON extraction moves to enhanced CTE; `transform_company_name()` and `transform_reserve_category()` macros also move to enhanced.
- **`stg_cc__forecast_outputs`**: Has JSON variant columns (best_forecast_params, ratio_params, type_curve_data). Keep as variant in staging; parsing belongs in downstream models.

### Current Compliance (0/11)

| Model | Current Pattern | Gaps |
|-------|----------------|------|
| Most models | source → renamed → final | Missing: config, filtered, enhanced, column grouping |
| `economic_runs` | source → surrogate_key → converted → final | Non-standard CTE names; no tags |
| `economic_one_liners` | source → surrogate_key → renamed → final | JSON extraction in renamed; no config |
| `forecast_outputs` | source → renamed → typed → final | Wrong tag names; WHERE in final |
| `daily_forecasts` | 5 CTEs but non-standard names | Heavy JSON parsing; completely custom structure |

---

## Out of Scope

- Data test additions beyond what exists in current YAML
- Materialization changes (all remain `view`)
- Downstream model changes
- Portable connector configuration changes

## Open Questions

None — all decisions resolved.

## Next Step

Run `/workflows:plan` to generate the implementation plan for Sprint 1 (context documentation).
