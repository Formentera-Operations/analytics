# Brainstorm: ProdView Staging Model Reorganization

**Date:** 2026-02-12
**Author:** Rob Stover
**Status:** Ready for planning

## What We're Building

Reorganize the 67 ProdView staging models from a flat directory into domain-based subdirectories that mirror ProdView's actual data model hierarchy. Each domain gets its own source YAML, staging schema YAML (with full column documentation), and co-located SQL models.

### Goals

1. **Directory structure communicates the data model** — someone browsing the folder tree immediately understands ProdView's domain boundaries
2. **Smaller, navigable YAML files** — instead of one monolithic source file and zero staging docs, each domain gets focused YAML
3. **Encode the "All Tables" Rosetta Stone** — the ProdView UI display name for every column becomes the staging YAML description, bridging backend column names to what users see in ProdView (and what drives quoted aliases in marts)
4. **Consistent testing pattern** — every staging model gets the same baseline tests

## Why This Approach

The ProdView data model has ~120+ tables across 14 functional domains. The current flat directory with 54 SQL files and a single source YAML makes it hard to understand which tables relate to each other, how the parent-child hierarchy works, or what columns mean without digging into each SQL file.

By organizing into domain folders that match the Breakdown doc's categorization, the project structure itself becomes documentation.

## Key Decisions

### 1. Domain Directory Structure

Organize into **10 domain folders** matching ProdView's functional domains:

```
models/operations/staging/prodview/
├── completions/       # Unit → Completion spine + surveillance (10 models)
│   ├── _src_prodview.yml      # Source tables for this domain
│   ├── _stg_prodview.yml      # Staging model docs + tests
│   ├── stg_prodview__units.sql
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
├── artificial_lift/   # Pump equipment + daily entries (5 models)
│   ├── _src_prodview.yml
│   ├── _stg_prodview.yml
│   ├── stg_prodview__artificial_lift.sql
│   ├── stg_prodview__rod_pump_configs.sql
│   ├── stg_prodview__rod_pump_entries.sql
│   ├── stg_prodview__plunger_lifts.sql
│   └── stg_prodview__plunger_lift_readings.sql
│
├── allocations/       # Production volumes + dispositions (7 models)
│   ├── _src_prodview.yml
│   ├── _stg_prodview.yml
│   ├── stg_prodview__daily_allocations.sql
│   ├── stg_prodview__monthly_allocations.sql
│   ├── stg_prodview__daily_dispositions.sql
│   ├── stg_prodview__monthly_dispositions.sql
│   ├── stg_prodview__gathered_daily_volumes.sql
│   ├── stg_prodview__unit_opening_inventories.sql
│   └── stg_prodview__fluid_levels.sql (if exists)
│
├── tanks/             # Tank master + readings + straps + volumes (8 models)
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
├── meters/            # All meter types + measurement points (8 models)
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
├── flow_network/      # Networks + nodes + connections + volumes (8 models)
│   ├── _src_prodview.yml
│   ├── _stg_prodview.yml
│   ├── stg_prodview__networks.sql
│   ├── stg_prodview__unit_nodes.sql
│   ├── stg_prodview__unit_node_connections.sql
│   ├── stg_prodview__node_daily_volumes.sql
│   ├── stg_prodview__node_monthly_volumes.sql
│   ├── stg_prodview__node_daily_corrections.sql
│   └── stg_prodview__node_monthly_corrections.sql (if exists)
│
├── facilities/        # Facility-level reporting (3 models)
│   ├── _src_prodview.yml
│   ├── _stg_prodview.yml
│   ├── stg_prodview__facility_daily_volumes.sql
│   ├── stg_prodview__facility_monthly_volumes.sql
│   └── stg_prodview__facility_daily_receipts.sql
│
├── routes/            # Field routes + user assignments (2 models)
│   ├── _src_prodview.yml
│   ├── _stg_prodview.yml
│   ├── stg_prodview__routes.sql
│   └── stg_prodview__route_users.sql
│
├── admin/             # Agreements, regulatory, remarks, tickets (5 models)
│   ├── _src_prodview.yml
│   ├── _stg_prodview.yml
│   ├── stg_prodview__partnership_agreements.sql
│   ├── stg_prodview__agreement_partners.sql
│   ├── stg_prodview__regulatory_reporting_keys.sql
│   ├── stg_prodview__remarks.sql
│   └── stg_prodview__tickets.sql
│
└── wiserock_tables/   # Raw metric values for WiseRock app (13 models, existing)
    ├── schema.yml     # (existing, keep as-is)
    └── stg_wiserock__pv_*.sql
```

**Decision rationale:** Units + Completions merged into `completions/` because Unit → Completion is ProdView's core spine. Everything downstream (status, downtimes, params, tests, targets) is completion-level surveillance. Keeping them together mirrors the hierarchy.

### 2. Source YAML: Per-Domain, Co-Located

- Each domain folder gets its own `_src_prodview.yml`
- All files define `name: prodview` with the same `database` and `schema` — dbt merges tables from the same source name
- Source YAML is lightweight: table name + brief description (1 line)
- Column-level documentation lives on **staging models only** (not sources)
- Delete the current monolithic `src_prodview.yml` once migration is complete
- Leading underscore (`_src_`) convention keeps YAML files sorted before SQL files

### 3. Staging YAML: Full Column Documentation

Each domain folder gets `_stg_prodview.yml` with:

- **Model description** — what the table represents in ProdView's hierarchy
- **Column descriptions** — ProdView UI display name from "All Tables" doc + business context
  - Identifiers: note what they link to (e.g., "FK to parent completion record")
  - Converted columns: note both the ProdView display name AND the unit conversion applied (e.g., "Tubing Pressure (converted from kPa to PSI)")
  - UserTxt/UserNum: document Formentera-specific usage (e.g., "BHA Type (PAGA/SAGA/RPGA) — Formentera customization")
  - System columns: brief standard descriptions (can be templated)
- **Tests** — match WiseRock pattern:
  - `unique` + `not_null` on IDREC (primary key)
  - `not_null` on IDFLOWNET
  - `accepted_values` on _fivetran_deleted / deleted flag

### 4. "All Tables" Doc as the Rosetta Stone

The Peloton ProdView Data Model - All Tables document maps every backend column name to its ProdView UI display name. This mapping is critical because:

- It's what users see in ProdView's interface
- It's what was used to create the quoted aliases in marts (e.g., `"Allocated Gas mcf"`)
- It bridges the gap between snake_case staging columns and business meaning

**Usage in YAML:** Each column description follows the pattern:
```yaml
- name: pres_tubing_psi
  description: "Tubing Pressure — converted from kPa to PSI. ProdView field: PresTub"
```

For UserTxt/UserNum fields with Formentera customizations:
```yaml
- name: user_text_1
  description: "BHA Type (PAGA/SAGA/RPGA) — Formentera customization of ProdView UserTxt1"
```

### 5. WiseRock Tables: Keep Separate

The `wiserock_tables/` subdirectory stays as-is. These are intentionally separate because:
- They use raw metric values (no unit conversion)
- They have the `stg_wiserock__pv_*` naming prefix (different source convention)
- They already have a complete `schema.yml`
- They serve a specific application (WiseRock well analytics)

### 6. Context Doc Update (context/sources/prodview.md)

Add a **Domain Map** section to the existing ProdView context doc showing which tables belong to which domain — matching the new directory structure. This replaces the need for developers to look at the "All Tables" docx.

**Do NOT duplicate column-level mapping in the context doc.** Instead, add a note:
> "For backend → frontend column mapping (ProdView UI display names), see the `_stg_prodview.yml` files in each domain folder under `models/operations/staging/prodview/`."

The staging YAML files are the single source of truth for column documentation.

## Open Questions

1. **Should `stg_prodview__fluid_levels` exist?** It's in the wiserock models but not sure if there's a main staging version. Need to verify against source tables.
2. **Node monthly corrections** — confirm `stg_prodview__node_monthly_corrections` exists as a separate model.
3. **Gas meters vs orifice meters** — the current models have both `gas_meters`/`gas_meter_readings` AND `gas_pd_meters`/`gas_pd_meter_readings`. Need to map these to the correct ProdView tables (pvUnitMeterOrifice vs pvUnitMeterPDGas).
4. **dbt_project.yml routing** — moving models into subdirectories may require updating the `models:` config in `dbt_project.yml` if schema/database routing relies on folder paths.

## Implementation Approach (High Level)

1. Create domain subdirectory structure
2. Move SQL files into appropriate domain folders
3. Split `src_prodview.yml` into per-domain source files
4. Build staging YAML files domain-by-domain using the "All Tables" doc
5. Update `dbt_project.yml` if needed for routing
6. Run `dbt parse --warn-error` to validate
7. Run `dbt build --select state:modified+` to verify nothing broke
8. Remove old `src_prodview.yml`

## Next Steps

Run `/workflows:plan` to create a detailed sprint plan with acceptance criteria for each domain.
