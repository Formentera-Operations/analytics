---
title: "WellView Staging 5-CTE Refactor — Sprint 3 Completion"
category: refactoring
tags: [wellview, staging, 5-cte, domain-organization, wv-macros, sprint-3, swarm, parallel-agents]
module: operations/staging/wellview
symptoms:
  - 29 staging models using legacy 2-CTE pattern (source_data, renamed)
  - No Fivetran deduplication (qualify row_number)
  - No trim/cast on text and date columns
  - Soft delete filter in source CTE instead of dedicated filtered CTE
  - No surrogate keys or _loaded_at metadata
  - No explicit output column contract (select * from renamed)
  - Computed flags mixed into renamed CTE instead of enhanced
date_solved: 2026-02-13
---

# WellView Staging 5-CTE Refactor — Sprint 3 Completion

## Problem

After Sprint 2 refactored 10 models in `general/`, `wellbore_surveys/`, and `operations/`, 29 models across 7 remaining domains still used the legacy 2-CTE pattern. These models lacked deduplication, proper type casting, surrogate keys, and explicit output contracts.

## What Sprint 3 Accomplished

- Refactored all 29 remaining WellView staging models to the 5-CTE pattern
- Applied consistent `trim()::varchar`, `::timestamp_ntz`, `::float`, `::boolean` casting
- Added Fivetran dedup via `qualify 1 = row_number() over (partition by idrec order by _fivetran_synced desc)`
- Replaced inline unit conversion math with `wv_*` macros (200+ conversions total)
- Moved computed flags from renamed to enhanced CTE
- Added surrogate keys and `_loaded_at` to all models
- Normalized config tags to `['wellview', 'staging', '{domain}']`
- Net: +6,093 / -2,783 lines across 29 files

## Domains and Models

| Domain | Models | Key Conversions |
|--------|:------:|-----------------|
| other | 1 | None (DBO source, not wellview_calcs) |
| zones_completions | 1 | 7 depth conversions |
| production_operations | 1 | Rate/volume/gas + 2 computed flags |
| reservoir_tests | 3 | Pressure, temperature, volume, depth |
| tubing_rods_equipment | 7 | Dimensions, torque, force, tensile, linear density |
| casing_cement | 8 | Dimensions, pressure, force, density, specialized inline |
| perfs_stims | 8 | 50+ conversions in perforations, 60+ in stimulation_intervals |

## Execution Strategy

### Parallel Agent Swarm

Sprint 3 used a swarm of 5 parallel agents, one per batch group:

1. **Batch 1** (other + zones_completions + production_operations): 3 simple models
2. **Batch 2** (reservoir_tests): 3 models — already in 5-CTE from Sprint 3 domain move commit
3. **Batch 3** (tubing_rods_equipment): 7 models — heaviest FK/connection fields
4. **Batch 4** (casing_cement): 8 models — already in 5-CTE from Sprint 3 domain move commit
5. **Batch 5** (perfs_stims): 8 models — most conversion-heavy (stimulation_intervals alone has 60+)

### Critical Learning: Sequential File Writes

The first launch of Batch 3 and 5 agents failed because they attempted parallel `Write` tool calls for multiple files simultaneously. When one Write was denied by the permission system, all sibling parallel Write calls errored out with "Sibling tool call errored."

**Fix:** Stopped failing agents, relaunched with explicit instruction: "Write files ONE AT A TIME, sequentially. Do NOT use parallel Write calls." Both new agents completed successfully.

**Rule for future swarms:** Agents that modify multiple files must write them sequentially, not in parallel. Parallel Read/Grep is fine; parallel Write causes cascading failures.

### Models Already Refactored

11 of the 29 models (reservoir_tests: 3, casing_cement: 8) were already in 5-CTE pattern from the Sprint 3 domain reorganization commit (`f882ab4`). The agents validated these rather than re-refactoring, saving time.

## Patterns Applied

### Conversion Macro Mapping

All inline division constants were replaced with named macros:

| Inline pattern | Macro | Unit |
|---------------|-------|------|
| `/ 0.3048` | `wv_meters_to_feet()` | Depth, length |
| `/ 0.0254` | `wv_meters_to_inches()` | Diameter, size |
| `/ 6.894757` | `wv_kpa_to_psi()` | Pressure |
| `/ 0.158987...` | `wv_cbm_to_bbl()` | Fluid volume |
| `/ 28.316...` | `wv_cbm_to_mcf()` | Gas volume |
| `/ 4.448...` | `wv_newtons_to_lbf()` | Force |
| `/ 4448.2...` | `wv_newtons_to_klbf()` | Tensile strength |
| `/ 1.488...` | `wv_kgm_to_lb_per_ft()` | Linear density |
| `/ 1.355...` | `wv_nm_to_ft_lb()` | Torque |
| `/ 0.04166...` | `wv_days_to_hours()` | Duration |
| `/ 0.000694...` | `wv_days_to_minutes()` | Duration |
| `/ 0.45359...` | `wv_kg_to_lb()` | Mass |
| `/ 0.555... + 32` | `wv_celsius_to_fahrenheit()` | Temperature |
| `/ 119.826...` | `wv_kgm3_to_lb_per_gal()` | Density |
| `/ 745.6999` | `wv_watts_to_hp()` | Power |
| `/ 9.869...e-13` | `wv_sqm_to_darcy()` | Permeability |
| `/ 0.001` | `wv_pas_to_cp()` | Viscosity |

### Kept Inline (No Macro)

Some conversions are too domain-specific for a shared macro:

- `/ 0.01` — percent (e.g., BSW, sand cut)
- `/ 1e-06` — ppm (e.g., salinity, H2S)
- `/ 45.359237` — sacks (cement-specific)
- `/ 22.620593832021` — psi/ft gradient
- Complex formulas (API gravity from density)

### Computed Flags in Enhanced CTE

Flags derived from raw values moved from renamed to enhanced:

```sql
-- enhanced CTE (not renamed)
coalesce(tapered_raw = 1, false) as is_tapered_string,
coalesce(centralized_raw = 1, false) as has_centralizers,
coalesce(regulatory = 1, false) as is_regulatory_issue,
```

The raw value is cast in renamed (e.g., `tapered::float as tapered_raw`), then the boolean flag is computed in enhanced.

## Verification

```bash
# Full parse with error promotion
dbt parse --warn-error --no-partial-parse  # PASSED

# Lint all 29 models
sqlfluff lint models/operations/staging/wellview/  # PASSED (0 violations)

# Pre-commit hooks
git commit  # sqlfluff-lint hook PASSED
```

## Related

- `docs/solutions/refactoring/wellview-staging-domain-refactor-sprint-2.md` — Sprint 2 patterns (10 models, 3 domains)
- `docs/solutions/logic-errors/wellview-cost-per-depth-rate-vs-length-conversion.md` — Rate vs length conversion gotcha
- `docs/solutions/refactoring/prodview-staging-5-cte-pattern.md` — Original 5-CTE pattern reference
- `docs/plans/federated-scribbling-tiger.md` — Sprint 3 execution plan
- PR #258 — Sprint 3 PR
