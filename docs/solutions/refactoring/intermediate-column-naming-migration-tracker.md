---
title: "Intermediate Model Column Naming — Migration Tracker"
category: refactoring
tags: [dbt, intermediate, quoted-aliases, snake_case, power-bi, migration]
module: operations/intermediate
symptoms:
  - Intermediate models using quoted aliases instead of snake_case
  - Inconsistent column naming between intermediate and mart layers
  - Need to trace which models are bridge vs modern pattern
date_created: 2026-02-12
---

# Intermediate Model Column Naming — Migration Tracker

## Context

The project convention is:

- **Staging** outputs **snake_case** (e.g., `allocated_oil_bbl`)
- **Intermediate** outputs **snake_case** (preferred) or quoted aliases as a temporary compatibility bridge
- **Marts** apply **quoted aliases** for Power BI consumers (e.g., `"Allocated Oil bbl"`)

Some intermediate models still output quoted aliases because their downstream mart/app consumers were built before the snake_case convention was established. These are "compatibility bridges" — they read snake_case from staging and output quoted aliases so downstream models don't need to change yet.

The goal is to eventually migrate all intermediate models to snake_case and push the quoted alias responsibility to the mart layer, where it belongs.

## Current State: Bridge Models (Quoted Aliases)

These 6 intermediate models still output quoted aliases. Each needs its downstream consumers migrated before it can switch to snake_case.

### Production

| Model | Downstream Consumers | Notes |
|-------|---------------------|-------|
| `int_prodview__production_volumes` | `fct_eng_volumes`, `int_fct_well_header` | Explicit bridge comment in file. Joins 4 staging models (allocations, downtimes, parameters, status). Largest bridge — 160+ quoted columns. |
| `int_prodview__tank_volumes` | `fct_eng_tank_inventories` | Joins tanks + tank daily volumes + units. |

### Finance (LOS v5)

| Model | Downstream Consumers | Notes |
|-------|---------------------|-------|
| `int_oda_afe_v2` | `los_v5_afe_v2` | AFE data for Lease Operating Statement. |
| `int_oda_calendar` | `los_v5_calendar` | Date dimension for LOS reporting. |
| `int_oda_gl` | `los_v5_gl` | General ledger for LOS. |
| `int_oda_wells` | `los_v5_wells`, `int_fct_well_header` | **Mixed** — some columns quoted, some snake_case. Needs cleanup. |

### Key dependency: `int_fct_well_header`

This model is a high-traffic hub that consumes two bridge models (`int_prodview__production_volumes`, `int_oda_wells`) and feeds:

- `fct_eng_well_header` (mart)
- `fct_eng_completion_downtimes` (mart, via `int_prodview__completion_downtimes`)
- `int_dim_asset_company` (intermediate)

Migrating `int_fct_well_header` to snake_case will require coordinating changes in both production and finance bridge models simultaneously.

## Already Migrated: Snake_case Models

These intermediate models already follow the modern snake_case convention. **No action needed.**

### Production (~15 models)
- `int_prodview__completion_downtimes` — snake_case, feeds `fct_eng_completion_downtimes`
- `int_prodview__production_targets` — snake_case
- `int_prodview__well_header` — snake_case
- `int_dim_prod_status` — snake_case, feeds `dim_eng_prod_status`
- `int_dim_asset_company` — snake_case
- `int_dim_route` — snake_case
- `int_fct_well_header` — snake_case (but reads quoted from upstream bridges)
- `int_cc__economic_runs` — snake_case
- `int_wellview__well_header` — snake_case
- `int_wellview__canonical_wells` — snake_case
- `int_wellview__well_status_history_bridge` — snake_case
- `int_wellview_job` — snake_case

### Finance (~20 models)
- `int_accounts_classified` — snake_case (materialized as table, high reuse)
- `int_general_ledger_enhanced` — snake_case (incremental)
- `int_gl_enhanced` — snake_case (incremental)
- `int_oda_ar_*` (8 AR models) — all snake_case
- `int_oda_latest_company_NRI` — snake_case
- `int_oda_latest_company_WI` — snake_case
- `int_cc__economic_runs_unpivoted` — pass-through
- `int_cc__forecast_*` (3 models) — pass-through/snake_case
- `int_economic_runs_with_one_liners` — pass-through
- `int_aegis__market_data` — pass-through
- `int_aegis__shrink_and_yield` — snake_case

### Griffin (~10 models)
- All `int_griffin__*` models — snake_case (built after convention was established)

### Well 360 (~7 models)
- All `int_well__*` models — snake_case (built after convention was established)

## Migration Playbook

When migrating a bridge model from quoted aliases to snake_case:

1. **Grep for all downstream consumers:**
   ```bash
   grep -r "ref('int_model_name')" models/ --include="*.sql"
   ```

2. **For each downstream mart model:** Add explicit `snake_col as "Quoted Alias"` mappings in the mart's SELECT list (like `fct_eng_completion_downtimes` does today).

3. **For each downstream intermediate model:** Update column references from `"Quoted Name"` to `snake_case`.

4. **Update the bridge model:** Remove quoted aliases, output snake_case.

5. **Validate:** `dbt build --select +int_model_name+` to build upstream and downstream.

6. **Update this tracker:** Move the model from "Bridge" to "Migrated" section.

### Suggested migration order

1. `int_oda_wells` — already mixed, clean up the inconsistency first
2. `int_oda_afe_v2`, `int_oda_calendar`, `int_oda_gl` — single downstream consumer each (LOS v5 marts)
3. `int_prodview__tank_volumes` — single downstream consumer
4. `int_prodview__production_volumes` — largest bridge, save for last. Requires coordinating `fct_eng_volumes` + `int_fct_well_header` + their downstreams

## Related

- Convention reference: `CLAUDE.md` → "Model Layers & Conventions" → Intermediate
- Staging 5-CTE pattern: `docs/solutions/refactoring/prodview-staging-5-cte-pattern.md`
- Sprint 5 PR (ProdView staging + downstream): PR #248
