---
title: "refactor: Reorganize ProdView staging models into domain-based subdirectories"
type: refactor
date: 2026-02-12
---

# Reorganize ProdView Staging Models into Domain-Based Subdirectories

## Overview

Reorganize 53 ProdView staging models from a flat `staging/prodview/` directory into 9 domain-based subdirectories that mirror ProdView's actual data model hierarchy. Each domain gets co-located source YAML, staging schema YAML (with full column documentation from the "All Tables" Rosetta Stone), and SQL models.

**Brainstorm:** [2026-02-12-prodview-staging-reorganization-brainstorm.md](../brainstorms/2026-02-12-prodview-staging-reorganization-brainstorm.md)

## Problem Statement

The current flat directory has 53 SQL files, one monolithic source YAML (`src_prodview.yml`), and **zero staging schema YAML files** for the main models. This means:
- No column-level documentation in the project for 53 staging models
- No way to understand which tables relate to each other without reading SQL
- The ProdView UI display names (the Rosetta Stone for mart quoted aliases) live only in an external docx
- No baseline tests on any main staging model (only WiseRock has schema YAML)

## Technical Approach

### Key Research Findings

1. **dbt_project.yml routing is safe.** The `prodview:` config at line 56 cascades into all subdirectories. No changes needed — already proven by `wiserock_tables/`.
2. **`ref()` resolves by model name, not file path.** Moving files won't break any of the 11 downstream intermediate consumers.
3. **No `.sqlfluff` changes needed.** No path-specific rules exist.
4. **43 of 53 models have zero downstream consumers.** Only 11 are referenced by intermediates. Low blast radius.
5. **Source YAML can be split.** dbt merges tables from the same source name across multiple YAML files.

### Institutional Learnings Applied

| Learning | How It Applies |
|----------|----------------|
| [Mart alias restoration](../solutions/refactoring/mart-quoted-alias-restoration-after-snake-case-refactor.md) | This reorg does NOT rename columns — only moves files and adds YAML. No mart impact. |
| [CI defer stale columns](../solutions/build-errors/ci-defer-stale-column-names.md) | No column renames = no CI defer staleness risk. |
| [Model deletion/rename procedure](../solutions/build-errors/dbt-model-deletion-rename-procedure.md) | We're moving, not renaming. dbt resolves by model name. But post-merge, old Snowflake views should be cleaned up if any paths changed. |
| [5-CTE pattern](../solutions/refactoring/prodview-staging-5-cte-pattern.md) | Column names in staging YAML should match the snake_case output of the refactored models. |

### Architecture: Target Directory Structure

```
models/operations/staging/prodview/
├── completions/                        # 10 models, 11 source tables
│   ├── _src_prodview.yml               # Units live here because Unit→Completion is ProdView's
│   ├── _stg_prodview.yml               # core spine. All child tables (status, params, tests,
│   ├── stg_prodview__units.sql         # downtimes, targets) are completion-level surveillance.
│   ├── stg_prodview__completions.sql
│   ├── stg_prodview__system_integrations.sql
│   ├── stg_prodview__status.sql
│   ├── stg_prodview__completion_downtimes.sql
│   ├── stg_prodview__completion_parameters.sql
│   ├── stg_prodview__production_tests.sql
│   ├── stg_prodview__production_tests_remaining.sql
│   ├── stg_prodview__production_targets.sql
│   └── stg_prodview__production_targets_daily.sql
│
├── artificial_lift/                    # 5 models, 5 source tables
│   ├── _src_prodview.yml
│   ├── _stg_prodview.yml
│   ├── stg_prodview__artificial_lift.sql
│   ├── stg_prodview__rod_pump_configs.sql
│   ├── stg_prodview__rod_pump_entries.sql
│   ├── stg_prodview__plunger_lifts.sql
│   └── stg_prodview__plunger_lift_readings.sql
│
├── allocations/                        # 6 models, 6 source tables
│   ├── _src_prodview.yml
│   ├── _stg_prodview.yml
│   ├── stg_prodview__daily_allocations.sql
│   ├── stg_prodview__monthly_allocations.sql
│   ├── stg_prodview__daily_dispositions.sql
│   ├── stg_prodview__monthly_dispositions.sql
│   ├── stg_prodview__gathered_daily_volumes.sql
│   └── stg_prodview__unit_opening_inventories.sql
│
├── tanks/                              # 7 models, 7 source tables
│   ├── _src_prodview.yml
│   ├── _stg_prodview.yml
│   ├── stg_prodview__tanks.sql
│   ├── stg_prodview__tank_readings.sql
│   ├── stg_prodview__tank_straps.sql
│   ├── stg_prodview__tank_strap_details.sql
│   ├── stg_prodview__tank_starting_inventories.sql
│   ├── stg_prodview__tank_monthly_volumes.sql
│   └── stg_prodview__tank_daily_volumes.sql
│
├── meters/                             # 8 models, 8 source tables
│   ├── _src_prodview.yml
│   ├── _stg_prodview.yml
│   ├── stg_prodview__liquid_meters.sql
│   ├── stg_prodview__liquid_meter_readings.sql
│   ├── stg_prodview__gas_pd_meters.sql
│   ├── stg_prodview__gas_pd_meter_readings.sql
│   ├── stg_prodview__gas_meters.sql
│   ├── stg_prodview__gas_meter_readings.sql
│   ├── stg_prodview__other_measurement_points.sql
│   └── stg_prodview__other_measurement_point_entries.sql
│
├── flow_network/                       # 7 models, 7 source tables
│   ├── _src_prodview.yml
│   ├── _stg_prodview.yml
│   ├── stg_prodview__networks.sql
│   ├── stg_prodview__unit_nodes.sql
│   ├── stg_prodview__unit_node_connections.sql
│   ├── stg_prodview__node_daily_volumes.sql
│   ├── stg_prodview__node_monthly_volumes.sql
│   ├── stg_prodview__node_daily_corrections.sql
│   └── stg_prodview__node_monthly_corrections.sql
│
├── facilities/                         # 3 models, 3 source tables
│   ├── _src_prodview.yml
│   ├── _stg_prodview.yml
│   ├── stg_prodview__facility_daily_volumes.sql
│   ├── stg_prodview__facility_monthly_volumes.sql
│   └── stg_prodview__facility_daily_receipts.sql
│
├── routes/                             # 2 models, 2 source tables
│   ├── _src_prodview.yml
│   ├── _stg_prodview.yml
│   ├── stg_prodview__routes.sql
│   └── stg_prodview__route_users.sql
│
├── admin/                              # 5 models, 5 source tables
│   ├── _src_prodview.yml
│   ├── _stg_prodview.yml
│   ├── stg_prodview__partnership_agreements.sql
│   ├── stg_prodview__agreement_partners.sql
│   ├── stg_prodview__regulatory_reporting_keys.sql
│   ├── stg_prodview__remarks.sql
│   └── stg_prodview__tickets.sql
│
└── wiserock_tables/                    # 13 models (existing, untouched)
    ├── schema.yml
    └── stg_wiserock__pv_*.sql
```

### Source Table → Model → Domain Mapping

| Domain | Source Table | Staging Model |
|--------|-------------|---------------|
| **completions** | PVT_PVUNIT | stg_prodview__units |
| **completions** | PVT_PVUNITCOMP | stg_prodview__completions |
| **completions** | PVT_PVSYSINTEGRATION | stg_prodview__system_integrations |
| **completions** | PVT_PVUNITCOMPSTATUS | stg_prodview__status |
| **completions** | PVT_PVUNITCOMPDOWNTM | stg_prodview__completion_downtimes |
| **completions** | PVT_PVUNITCOMPPARAM | stg_prodview__completion_parameters |
| **completions** | PVT_PVUNITCOMPTEST | stg_prodview__production_tests |
| **completions** | PVT_PVUNITCOMPTESTREQEXCALC | stg_prodview__production_tests_remaining |
| **completions** | PVT_PVUNITCOMPTARGET | stg_prodview__production_targets |
| **completions** | PVT_PVUNITCOMPTARGETDAY | stg_prodview__production_targets_daily |
| **completions** | PVT_PVUNITCOMPFLUIDLEVEL | *(source only — no staging model yet)* |
| **artificial_lift** | PVT_PVUNITCOMPPUMP | stg_prodview__artificial_lift |
| **artificial_lift** | PVT_PVUNITCOMPPUMPROD | stg_prodview__rod_pump_configs |
| **artificial_lift** | PVT_PVUNITCOMPPUMPRODENTRY | stg_prodview__rod_pump_entries |
| **artificial_lift** | PVT_PVUNITCOMPPUMPPLUNGER | stg_prodview__plunger_lifts |
| **artificial_lift** | PVT_PVUNITCOMPPUMPPLUNGERENTRY | stg_prodview__plunger_lift_readings |
| **allocations** | PVT_PVUNITALLOCMONTHDAY | stg_prodview__daily_allocations |
| **allocations** | PVT_PVUNITALLOCMONTH | stg_prodview__monthly_allocations |
| **allocations** | PVT_PVUNITDISPMONTH | stg_prodview__monthly_dispositions |
| **allocations** | PVT_PVUNITDISPMONTHDAY | stg_prodview__daily_dispositions |
| **allocations** | PVT_PVUNITCOMPGATHMONTHDAYCALC | stg_prodview__gathered_daily_volumes |
| **allocations** | PVT_PVOPENSTATEUNIT | stg_prodview__unit_opening_inventories |
| **tanks** | PVT_PVUNITTANK | stg_prodview__tanks |
| **tanks** | PVT_PVUNITTANKENTRY | stg_prodview__tank_readings |
| **tanks** | PVT_PVUNITTANKSTRAP | stg_prodview__tank_straps |
| **tanks** | PVT_PVUNITTANKSTRAPDATA | stg_prodview__tank_strap_details |
| **tanks** | PVT_PVUNITTANKSTARTINV | stg_prodview__tank_starting_inventories |
| **tanks** | PVT_PVUNITTANKMONTHCALC | stg_prodview__tank_monthly_volumes |
| **tanks** | PVT_PVUNITTANKMONTHDAYCALC | stg_prodview__tank_daily_volumes |
| **meters** | PVT_PVUNITMETERLIQUID | stg_prodview__liquid_meters |
| **meters** | PVT_PVUNITMETERLIQUIDENTRY | stg_prodview__liquid_meter_readings |
| **meters** | PVT_PVUNITMETERPDGAS | stg_prodview__gas_pd_meters |
| **meters** | PVT_PVUNITMETERPDGASENTRY | stg_prodview__gas_pd_meter_readings |
| **meters** | PVT_PVUNITMETERORIFICE | stg_prodview__gas_meters |
| **meters** | PVT_PVUNITMETERORIFICEENTRY | stg_prodview__gas_meter_readings |
| **meters** | PVT_PVUNITMEASPT | stg_prodview__other_measurement_points |
| **meters** | PVT_PVUNITMEASPTENTRY | stg_prodview__other_measurement_point_entries |
| **flow_network** | PVT_PVFLOWNETHEADER | stg_prodview__networks |
| **flow_network** | PVT_PVUNITNODE | stg_prodview__unit_nodes |
| **flow_network** | PVT_PVUNITNODEFLOWTO | stg_prodview__unit_node_connections |
| **flow_network** | PVT_PVUNITNODEMONTHDAYCALC | stg_prodview__node_daily_volumes |
| **flow_network** | PVT_PVUNITNODEMONTHCALC | stg_prodview__node_monthly_volumes |
| **flow_network** | PVT_PVUNITNODECORR | stg_prodview__node_monthly_corrections |
| **flow_network** | PVT_PVUNITNODECORRDAY | stg_prodview__node_daily_corrections |
| **facilities** | PVT_PVFACILITYMONTHDAYCALC | stg_prodview__facility_daily_volumes |
| **facilities** | PVT_PVFACILITYMONTHCALC | stg_prodview__facility_monthly_volumes |
| **facilities** | PVT_PVFACRECDISPCALC | stg_prodview__facility_daily_receipts |
| **routes** | PVT_PVROUTESETROUTE | stg_prodview__routes |
| **routes** | PVT_PVROUTESETROUTEUSERID | stg_prodview__route_users |
| **admin** | PVT_PVUNITAGREEMT | stg_prodview__partnership_agreements |
| **admin** | PVT_PVUNITAGREEMTPARTNER | stg_prodview__agreement_partners |
| **admin** | PVT_PVUNITREGBODYKEY | stg_prodview__regulatory_reporting_keys |
| **admin** | PVT_PVUNITREMARK | stg_prodview__remarks |
| **admin** | PVT_PVTICKET | stg_prodview__tickets |

### Downstream Consumer Map (Blast Radius)

Only 11 of 53 models have intermediate consumers. Listed by reference count:

| Staging Model | # Consumers | Consumer Models |
|---------------|-------------|-----------------|
| stg_prodview__units | 6 | int_prodview__well_header, int_prodview__tank_volumes, int_well__spine, int_well__prodview, int_wellview__well_header, int_prodview__production_volumes (indirect) |
| stg_prodview__daily_allocations | 4 | int_prodview__production_volumes, int_well__prodview, int_well__first_production |
| stg_prodview__status | 3 | int_prodview__production_volumes, int_dim_prod_status, int_well__prodview |
| stg_prodview__system_integrations | 3 | int_prodview__well_header, int_wellview__well_header |
| stg_prodview__completion_downtimes | 2 | int_prodview__production_volumes, int_prodview__completion_downtimes |
| stg_prodview__completions | 1 | int_prodview__well_header |
| stg_prodview__completion_parameters | 1 | int_prodview__production_volumes |
| stg_prodview__tanks | 1 | int_prodview__tank_volumes |
| stg_prodview__tank_daily_volumes | 1 | int_prodview__tank_volumes |
| stg_prodview__routes | 1 | int_dim_route |
| stg_prodview__production_targets / _daily | 1 | int_prodview__production_targets |

**All other 42 models** have zero `ref()` consumers. No blast radius.

## Implementation Phases

### Phase 1: Directory Structure + File Migration

Create all 9 domain subdirectories and move SQL files. This is pure file movement — no content changes.

#### Tasks

- [ ] **1.1** Create 9 domain subdirectories under `models/operations/staging/prodview/`
  - `completions/`, `artificial_lift/`, `allocations/`, `tanks/`, `meters/`, `flow_network/`, `facilities/`, `routes/`, `admin/`

- [ ] **1.2** Move SQL files into domain folders using `git mv` (preserves history)
  - `completions/`: 10 files (units, completions, system_integrations, status, completion_downtimes, completion_parameters, production_tests, production_tests_remaining, production_targets, production_targets_daily)
  - `artificial_lift/`: 5 files (artificial_lift, rod_pump_configs, rod_pump_entries, plunger_lifts, plunger_lift_readings)
  - `allocations/`: 6 files (daily_allocations, monthly_allocations, daily_dispositions, monthly_dispositions, gathered_daily_volumes, unit_opening_inventories)
  - `tanks/`: 7 files (tanks, tank_readings, tank_straps, tank_strap_details, tank_starting_inventories, tank_monthly_volumes, tank_daily_volumes)
  - `meters/`: 8 files (liquid_meters, liquid_meter_readings, gas_pd_meters, gas_pd_meter_readings, gas_meters, gas_meter_readings, other_measurement_points, other_measurement_point_entries)
  - `flow_network/`: 7 files (networks, unit_nodes, unit_node_connections, node_daily_volumes, node_monthly_volumes, node_daily_corrections, node_monthly_corrections)
  - `facilities/`: 3 files (facility_daily_volumes, facility_monthly_volumes, facility_daily_receipts)
  - `routes/`: 2 files (routes, route_users)
  - `admin/`: 5 files (partnership_agreements, agreement_partners, regulatory_reporting_keys, remarks, tickets)

- [ ] **1.3** Run `dbt parse --warn-error` to verify all 53 models are found and all `ref()` calls resolve

**Acceptance criteria:** All 53 SQL files are in domain folders. `dbt parse --warn-error` passes. No SQL files remain in `prodview/` root (only `src_prodview.yml` until Phase 2).

### Phase 2: Source YAML Split

Split the monolithic `src_prodview.yml` into 9 per-domain source files. Then remove the original.

#### Source YAML Template

Each domain's `_src_prodview.yml` follows this structure:

```yaml
# Source: prodview — database/schema MUST match all other _src_prodview.yml files.
# This is a split-source pattern: dbt merges tables from files sharing the same source name.
# If you change database or schema, update ALL 9 _src_prodview.yml files.
version: 2

sources:
  - name: prodview
    database: PELOTON_FORMENTERAOPS_FORMENTERAOPS_PV30
    schema: FORMENTERAOPS_PV30_DBO
    tables:
      - name: PVT_PVUNIT
        description: "Units (wells/pads) — building blocks of the flow network"
      - name: PVT_PVUNITCOMP
        description: "Completions — producing intervals within units"
```

**Key rules:**
- All 9 files define `name: prodview` with the same `database` and `schema` — dbt merges them. This is the project's first use of the split-source pattern (WellView uses separate source names, not a shared one). Each file MUST include the 3-line header comment above to prevent configuration drift.
- Table descriptions are 1-line summaries (column docs go in staging YAML)
- Existing column-level docs on artificial lift source tables (PVT_PVUNITCOMPPUMP, PVT_PVUNITCOMPPUMPROD, PVT_PVUNITCOMPPUMPRODENTRY) move to the staging YAML instead
- Include `PVT_PVUNITCOMPFLUIDLEVEL` in completions/ source even though no staging model exists yet

#### Tasks

- [ ] **2.1** Create `_src_prodview.yml` for each of the 9 domain folders with the correct source tables (per mapping table above). Include the 3-line header comment in every file.
- [ ] **2.2** Run `dbt parse --warn-error` **with the original `src_prodview.yml` still present** — this validates the new files parse correctly. If any table appears in both the original and a new file, dbt will error on duplicate table names. Fix before proceeding.
- [ ] **2.3** Delete original `src_prodview.yml` from prodview root
- [ ] **2.4** Run `dbt parse --warn-error` again to confirm no orphaned source references after deletion

**Acceptance criteria:** 9 `_src_prodview.yml` files exist, one per domain (each with the canonical source header comment). Original `src_prodview.yml` is deleted. `dbt parse --warn-error` passes.

### Phase 3: Staging Schema YAML Creation

Create `_stg_prodview.yml` per domain with full column documentation. This is the heaviest phase — each model needs column descriptions sourced from the "All Tables" document.

#### Staging YAML Template

```yaml
version: 2

models:
  - name: stg_prodview__units
    description: >
      ProdView units — the building blocks of the flow network. A "unit" is ProdView's
      term for a well, pad, or producing entity. Most Formentera units are single wells
      with one completion. Source: PVT_PVUNIT.
    columns:
      # -- identifiers
      - name: unit_sk
        description: "Surrogate key (MD5 hash of id_rec)"
      - name: id_rec
        description: "Unique record ID (PK). ProdView: IDRec"
        data_tests:
          - not_null
          - unique
      - name: id_flownet
        description: "Flow network identifier. ProdView: IDFlowNet"
        data_tests:
          - not_null

      # -- descriptive fields
      - name: unit_name
        description: "Unit Name. ProdView UI: 'Name'"
      - name: display_name
        description: "Display Name. ProdView UI: 'Short Name'"

      # -- Formentera-specific identifiers (UserTxt/UserNum customizations)
      - name: user_text_2
        description: "Completion Status — Formentera customization of ProdView UserTxt2"

      # -- converted fields
      - name: elevation_ft
        description: "Ground Elevation (converted from meters to feet). ProdView: Elevation"

      # -- system / audit
      - name: modified_at_utc
        description: "Last modification date (UTC). ProdView: sysModDate"
      - name: created_at_utc
        description: "Creation date (UTC). ProdView: sysCreateDate"

      # -- dbt metadata
      - name: _fivetran_deleted
        description: "Fivetran soft delete flag"
      - name: _loaded_at
        description: "dbt load timestamp"
```

**Key conventions:**
- `version: 2` at top (standardize across project)
- Column names match **staging model output** (snake_case from refactored models)
- Descriptions include ProdView UI display name from "All Tables" doc
- Converted columns note both the ProdView field name AND the conversion applied
- UserTxt/UserNum fields document Formentera-specific customization
- Comment headers group columns: `# -- identifiers`, `# -- descriptive fields`, `# -- dates`, `# -- converted fields`, `# -- system / audit`, `# -- dbt metadata`
- System/audit and dbt metadata columns use standardized descriptions (see "System/Audit Column Templating" in Implementation Notes)
- Tests: `unique` + `not_null` on PK (typically `id_rec`), `not_null` on `id_flownet`

#### Column Documentation Source

The "Peloton ProdView Data Model - All Tables" document at `~/Documents/Recents/Peloton Prodview Data Model - All Tables.docx` is the authoritative mapping. For each model:

1. Find the ProdView internal table name (e.g., `pvUnitCompParam`)
2. Extract all columns with their UI display names and data types
3. Map to the staging model's snake_case output column names
4. Add unit conversion notes where applicable (reference `macros/prodview_helpers/prodview_unit_conversions.sql`)
5. Add Formentera-specific meanings for UserTxt/UserNum fields (reference `context/sources/prodview.md` § User-Defined Fields)

#### Tasks — By Priority (downstream consumers first)

**Priority 1: High-traffic models (have downstream consumers)**

- [ ] **3.1** Create `completions/_stg_prodview.yml`
  - 10 models: units, completions, system_integrations, status, completion_downtimes, completion_parameters, production_tests, production_tests_remaining, production_targets, production_targets_daily
  - Read each model's SQL to get the output column names
  - Map each column to its ProdView UI name using "All Tables" doc
  - Document Formentera UserTxt/UserNum customizations from `context/sources/prodview.md`

- [ ] **3.2** Create `allocations/_stg_prodview.yml`
  - 6 models: daily_allocations, monthly_allocations, daily_dispositions, monthly_dispositions, gathered_daily_volumes, unit_opening_inventories
  - daily_allocations is the most-queried table — extra care on volume column descriptions

- [ ] **3.3** Create `artificial_lift/_stg_prodview.yml`
  - 5 models: artificial_lift, rod_pump_configs, rod_pump_entries, plunger_lifts, plunger_lift_readings
  - Move existing column docs from `src_prodview.yml` (artificial lift tables) into this file

- [ ] **3.4** Create `tanks/_stg_prodview.yml`
  - 7 models: tanks, tank_readings, tank_straps, tank_strap_details, tank_starting_inventories, tank_monthly_volumes, tank_daily_volumes

**Priority 2: Lower-traffic models (no downstream consumers)**

- [ ] **3.5** Create `meters/_stg_prodview.yml`
  - 8 models: liquid_meters, liquid_meter_readings, gas_pd_meters, gas_pd_meter_readings, gas_meters, gas_meter_readings, other_measurement_points, other_measurement_point_entries
  - Note: `gas_meters` = ProdView orifice meters (PVT_PVUNITMETERORIFICE)

- [ ] **3.6** Create `flow_network/_stg_prodview.yml`
  - 7 models: networks, unit_nodes, unit_node_connections, node_daily_volumes, node_monthly_volumes, node_daily_corrections, node_monthly_corrections

- [ ] **3.7** Create `facilities/_stg_prodview.yml`
  - 3 models: facility_daily_volumes, facility_monthly_volumes, facility_daily_receipts

- [ ] **3.8** Create `routes/_stg_prodview.yml`
  - 2 models: routes, route_users

- [ ] **3.9** Create `admin/_stg_prodview.yml`
  - 5 models: partnership_agreements, agreement_partners, regulatory_reporting_keys, remarks, tickets

**Acceptance criteria per task:** YAML file passes yamllint. `dbt parse --warn-error` passes. Every column in the staging model's `final` CTE has a corresponding entry in the YAML.

### Phase 4: Context Doc + Cleanup

Update the ProdView context doc and clean up.

#### Tasks

- [ ] **4.1** Add **Domain Map** section to `context/sources/prodview.md`
  - Table showing all ProdView domains, their source tables, and staging model locations
  - Note pointing to `_stg_prodview.yml` files as the source of truth for column documentation
  - Update the "Existing dbt Models" section to reflect the new directory structure

- [ ] **4.2** Verify no files remain in `prodview/` root (should only be `wiserock_tables/` and 9 domain folders)

- [ ] **4.3** Run `yamllint -c .yamllint.yml` on all new YAML files

**Acceptance criteria:** Context doc updated. No orphaned files in prodview root. All YAML passes yamllint.

### Phase 5: Validation

Full validation pass.

#### Tasks

- [ ] **5.1** `dbt parse --warn-error` — all models and sources resolve
- [ ] **5.2** `dbt compile --select tag:prodview` or `dbt compile --select path:models/operations/staging/prodview` — all 53+13 models compile
- [ ] **5.3** `dbt build --select state:modified+` — build changed models + downstream; verify no failures
- [ ] **5.4** Spot-check 3 models with `dbt show` to verify data still flows:
  - `stg_prodview__units` (most downstream consumers)
  - `stg_prodview__daily_allocations` (most business-critical)
  - `stg_prodview__rod_pump_entries` (recently refactored)
- [ ] **5.5** `sqlfluff lint` on a sample of moved files to confirm no lint regressions
- [ ] **5.6** `git push` — CI pipeline must pass

**Acceptance criteria:** All validation steps pass. CI green. No downstream model failures.

## Acceptance Criteria

### Functional Requirements

- [ ] 53 SQL files organized into 9 domain subdirectories
- [ ] 9 `_src_prodview.yml` files (one per domain) replacing the monolithic `src_prodview.yml`
- [ ] 9 `_stg_prodview.yml` files with full column documentation for all 53 models
- [ ] Column descriptions include ProdView UI display names from "All Tables" doc
- [ ] Formentera-specific UserTxt/UserNum customizations documented
- [ ] Unit conversion notes on all converted columns
- [ ] Baseline tests on every model (unique+not_null on PK, not_null on id_flownet)
- [ ] `context/sources/prodview.md` updated with domain map
- [ ] WiseRock tables untouched

### Quality Gates

- [ ] `dbt parse --warn-error` passes
- [ ] `dbt build --select state:modified+` passes (no downstream failures)
- [ ] All YAML passes `yamllint`
- [ ] `sqlfluff lint` passes on moved files
- [ ] CI pipeline green
- [ ] `git push` succeeds

## Dependencies & Prerequisites

- **Requires:** The "Peloton ProdView Data Model - All Tables" docx (`~/Documents/Recents/`) for column mapping
- **Requires:** The "ProdView Data Model Breakdown" docx (`~/Downloads/AI Library/Documents/`) for domain categorization
- **Requires:** `context/sources/prodview.md` for Formentera-specific UserTxt/UserNum mappings
- **No blockers:** This is a pure refactoring task with no external dependencies

## Risk Analysis & Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| dbt source merge conflicts across YAML files | Low | Medium | All files define `name: prodview` with identical database/schema. dbt merges tables. Tested with `dbt parse`. |
| Column name mismatch between YAML and SQL | Medium | Low | For each model, read the `final` CTE to get exact output column names. Run `dbt parse --warn-error` which catches mismatches if tests reference nonexistent columns. |
| CI fails due to path changes | Very Low | Low | dbt resolves by model name, not path. Proven by existing `wiserock_tables/` precedent. |
| "All Tables" docx column names don't match current staging SQL | Low | Medium | Cross-reference with actual SQL. The docx is the ProdView system's native column names; the staging models renamed to snake_case. Map via the original source column names. |
| Large PR size | High | Low | This is a refactoring-only PR. No logic changes. Consider splitting into 2 PRs (see Implementation Notes below). Reviewers can focus on YAML quality, not code changes. |
| Heavy CI run from `state:modified+` | High | Low | After `git mv`, dbt detects all 53 models as modified (new `original_file_path` in manifest). CI will build all 53 staging views + ~11 downstream intermediate/mart consumers (~64 models). Expect a longer-than-normal CI run. |

## Implementation Notes

### PR Strategy

This plan produces 72+ file changes (53 `git mv` + 9 source YAMLs + 9 staging YAMLs + 1 deletion). GitHub's diff UI gets sluggish above ~50 files. Consider splitting:

- **PR #1 (Phases 1+2):** File moves + source YAML split. Pure mechanical — easy to review, low risk.
- **PR #2 (Phases 3+4+5):** Staging YAML creation + context doc + validation. Documentation-only — reviewers focus on column accuracy.

### System/Audit Column Templating

Every ProdView staging model ends with the same ~8 system/audit and dbt metadata columns (`created_by`, `created_at_utc`, `modified_by`, `modified_at_utc`, `_fivetran_deleted`, `_fivetran_synced`, `_loaded_at`). To reduce copy-paste drift across 53 models' YAML:

1. Use identical descriptions for these columns across all `_stg_prodview.yml` files
2. Keep a reference block in this plan (or a shared comment) that can be copied verbatim
3. Focus documentation effort on the domain-specific columns (identifiers, descriptive fields, converted fields, Formentera customizations)

Standard system/audit column descriptions:
```yaml
      # -- system / audit
      - name: created_by
        description: "User who created the record. ProdView: sysCreateUser"
      - name: created_at_utc
        description: "Record creation timestamp (UTC). ProdView: sysCreateDate"
      - name: modified_by
        description: "User who last modified the record. ProdView: sysModUser"
      - name: modified_at_utc
        description: "Last modification timestamp (UTC). ProdView: sysModDate"

      # -- dbt metadata
      - name: _fivetran_deleted
        description: "Fivetran soft delete flag"
      - name: _fivetran_synced
        description: "Fivetran sync timestamp"
      - name: _loaded_at
        description: "dbt model load timestamp (current_timestamp at build time)"
```

### Model Count Reconciliation

The brainstorm references "67 ProdView staging models" — this includes 13 WiseRock models (untouched) and 1 source-only table (`PVT_PVUNITCOMPFLUIDLEVEL`, no staging model). This plan scopes to the **53 non-WiseRock staging models** being reorganized. The brainstorm also counts "10 domain folders" including WiseRock; this plan works with **9 new domains** (WiseRock stays as-is).

## References & Research

### Internal References
- Brainstorm: [docs/brainstorms/2026-02-12-prodview-staging-reorganization-brainstorm.md](../brainstorms/2026-02-12-prodview-staging-reorganization-brainstorm.md)
- ProdView context: [context/sources/prodview.md](../../context/sources/prodview.md)
- dbt_project.yml routing: [dbt_project.yml:56](../../dbt_project.yml#L56) — `prodview:` config
- WiseRock YAML template: [models/operations/staging/prodview/wiserock_tables/schema.yml](../../models/operations/staging/prodview/wiserock_tables/schema.yml)
- Unit conversion macros: [macros/prodview_helpers/prodview_unit_conversions.sql](../../macros/prodview_helpers/prodview_unit_conversions.sql)
- Mart alias restoration: [docs/solutions/refactoring/mart-quoted-alias-restoration-after-snake-case-refactor.md](../solutions/refactoring/mart-quoted-alias-restoration-after-snake-case-refactor.md)
- CI defer staleness: [docs/solutions/build-errors/ci-defer-stale-column-names.md](../solutions/build-errors/ci-defer-stale-column-names.md)

### External References
- "Peloton ProdView Data Model - All Tables" — `~/Documents/Recents/Peloton Prodview Data Model - All Tables.docx`
- "ProdView Data Model Breakdown" — `~/Downloads/AI Library/Documents/ProdView_Data_Model_Breakdown.docx`
