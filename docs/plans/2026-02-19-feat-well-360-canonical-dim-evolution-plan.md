# well_360 → Canonical Well Dimension — Sprint Plan

**Date:** 2026-02-19
**Branch:** `feature/well-360-canonical-dim`
**Status:** READY
**Related brainstorm:** `docs/brainstorms/2026-02-18-well-performance-mart-brainstorm.md`

---

## Goal

Evolve `well_360` into the project's single canonical well dimension by:

1. Absorbing the valuable derived logic from `dim_wells` (basin classification,
   activity status, revenue flags, well type from name patterns)
2. Surfacing ODA's internal `well_id` for backward-compatible joins to `fct_los`
3. Expanding `int_well__oda` to pass through the missing ODA attributes
4. Deleting `dim_wells` (zero downstream SQL consumers — migration cost is free)

`dim_wells` is an ODA-accidental dimension built to serve the finance/LOS use case
before `well_360` existed. It has no `ref()` consumers. Its valuable logic belongs
in `well_360` as the enterprise-wide canonical well dimension.

---

## Gap Analysis

### What `dim_wells` has that `well_360` is missing

| Attribute | Source | Priority |
|-----------|--------|----------|
| `well_id` (ODA internal numeric ID) | `stg_oda__wells.id` | **Critical** — needed for `fct_los` joins |
| `basin_name` (hardcoded state/county → basin) | `dim_wells` CASE logic | **High** — Formentera's canonical basin classification |
| `is_revenue_generating` | ODA status + billing flags | **High** — operational reporting flag |
| `is_hold_billing` / `is_suspend_revenue` | `stg_oda__wells` | **High** — financial control flags |
| `activity_status` (Producing/Shut In/P&A rollup) | ODA status fields | **Medium** — may merge with `unified_status` |
| `op_ref` (standardized operated status string) | `property_reference_code` | **Medium** — richer than boolean `is_operated` |
| `well_type` from name patterns (Horizontal/SWD/Injector) | Name regex logic | **Medium** — complement to `well_configuration_type` |
| `search_key` (UF-SEARCH KEY userfield) | `stg_oda__userfield` | **Medium** — used in GL reporting |
| `pv_field` (UF-PV FIELD userfield) | `stg_oda__userfield` | **Low** |
| `cost_center_type_code` / `cost_center_type_name` | `stg_oda__wells` | **Low** |
| `is_well` flag (derived from cost_center_type) | `stg_oda__wells` | **Low** |
| `operating_group_code` / `operating_group_name` | `stg_oda__wells` | **Low** |
| `production_status_name` (raw ODA status) | `stg_oda__wells` | **Low** |

### What `well_360` has that `dim_wells` does NOT (no migration needed)
- Multi-source golden record (WellView, ProdView, ComboCurve, Enverus)
- Source lineage tracking + conflict detection + data quality scoring
- `unified_status` (normalized across all sources)
- Depth/completion metrics (lateral length, TVD, MD)
- Enverus production benchmarks + completion data
- Full date set (permit, spud, rig release, completion, first production)
- Coordinates (surface + bottom hole)
- Source presence flags (`in_oda`, `in_prodview`, etc.)

---

## Implementation Plan

### Step 1: Expand `int_well__oda`

Add the missing ODA columns to `int_well__oda` so they flow through to `well_360`.
Read `stg_oda__wells` to confirm column names before writing.

**Add to `int_well__oda`:**
```sql
w.id as well_id,                         -- ODA internal numeric ID (critical for fct_los joins)
w.property_reference_code as op_nonop_code,  -- already present — keep
w.well_status_type_code,
w.production_status_name,
w.is_hold_all_billing,
w.is_suspend_all_revenue,
w.cost_center_type_code,
w.cost_center_type_name,
w.operating_group_code,
w.operating_group_name,
```

Add userfield lookups for `search_key` and `pv_field` (same pattern as `dim_wells`
already uses — pivot `stg_oda__userfield` on `UF-SEARCH KEY` and `UF-PV FIELD`).

### Step 2: Add derived attributes to `well_360`

In the `golden_record` CTE, add the following (sourced from `oda`):

**`oda_well_id`** — the ODA internal `well_id` needed for backward-compat joins:
```sql
oda.well_id as oda_well_id,
```

**`basin_name`** — migrate the hardcoded state/county CASE from `dim_wells` verbatim.
This is Formentera's canonical basin classification. Keep `well_360`'s existing
`geological_basin` (from Enverus/WellView) alongside it — they serve different purposes:
- `basin_name` = Formentera's operational/accounting basin grouping (ODA state/county)
- `geological_basin` = technical geological basin from Enverus/WellView

**`is_revenue_generating`** — derive from ODA flags:
```sql
coalesce(
    oda.well_status_type_name = 'Producing'
    and oda.production_status_name = 'Active'
    and not oda.is_hold_all_billing
    and not oda.is_suspend_all_revenue,
    false
) as is_revenue_generating,
```

**`is_hold_billing`** / **`is_suspend_revenue`** — pass through directly from ODA:
```sql
coalesce(oda.is_hold_all_billing, false) as is_hold_billing,
coalesce(oda.is_suspend_all_revenue, false) as is_suspend_revenue,
```

**`op_ref`** — the standardized operated status string from `dim_wells`:
```sql
case
    when oda.op_nonop_code = 'NON-OPERATED' then 'NON-OPERATED'
    when oda.op_nonop_code in ('OPERATED', 'Operated') then 'OPERATED'
    when oda.op_nonop_code = 'CONTRACT_OP' then 'CONTRACT OPERATED'
    when oda.op_nonop_code in ('DNU', 'ACCOUNTING', 'Accounting') then 'NON-WELL'
    when oda.op_nonop_code in ('OTHER', 'Other') then 'OTHER'
    when oda.op_nonop_code = 'MIDSTREAM' then 'MIDSTREAM'
    else 'UNKNOWN'
end as op_ref,
```

**`well_type_oda`** — well type derived from name patterns (from `dim_wells`).
Name it `well_type_oda` to distinguish from `well_type` (ComboCurve/Enverus):
```sql
case
    when upper(well_name) like '%SWD%' or upper(well_name) like '%DISPOSAL%' then 'SWD'
    when unified_status = 'INJECTOR' or upper(well_name) like '%INJ%' then 'Injector'
    when regexp_like(upper(well_name), '.*[0-9]+[MW]?X?H(-[A-Z0-9]+)?$') then 'Horizontal'
    when upper(well_name) like '%H-LL%' or upper(well_name) like '%H-SL%' then 'Horizontal'
    when upper(well_name) like '%UNIT%' then 'Unit Well'
    when oda.cost_center_type_name = 'Well' then 'Vertical/Conventional'
    else 'Other'
end as well_type_oda,
```

**`search_key`** and **`pv_field`** — ODA userfield attributes:
```sql
oda.search_key,
oda.pv_field,
```

**`activity_status`** — consider merging with `unified_status` rather than adding
a separate column. `unified_status` uses normalized values (PRODUCING, SHUT IN, etc.);
`activity_status` from `dim_wells` uses sentence-case (Producing, Shut In, etc.).
Decide at implementation: either rename to align or add as a separate alias.

### Step 3: Validate and delete `dim_wells`

1. Confirm `grep -r "dim_wells" models/` returns only the comment in `stg_oda__userfield.sql`
   (no actual `ref()` calls)
2. Run `dbt build --select well_360 --full-refresh` — full refresh required because
   new columns are being added to a materialized table
3. Spot-check: verify `oda_well_id` populates and matches known wells from `fct_los`
4. Delete `models/operations/marts/finance/dim_wells.sql` and its YAML entry
5. Run `dbt build --select state:modified+` to confirm no downstream breakage

### Step 4: Update `int_well__oda` YAML docs

Add column descriptions for all new attributes. Minimum: `description` per column.

---

## Key Constraints

**Full refresh required:** Adding columns to `well_360` (a materialized table)
requires `dbt build --select well_360 --full-refresh`. The table has 8,045 rows
and is fast to rebuild.

**`oda_well_id` naming:** Use `oda_well_id` (not `well_id`) to avoid ambiguity —
`well_360` already has system-specific IDs prefixed by source (`wellview_id`,
`prodview_unit_id`, `enverus_well_id`). ODA's internal ID should follow the same pattern.

**`basin_name` vs `geological_basin`:** Keep both. `basin_name` = Formentera operational
grouping (ODA state/county hardcoded). `geological_basin` = technical from Enverus/WellView.
These serve different teams and should coexist.

**Do NOT merge `dim_wells` into `well_360` as a SQL dependency.** Migrate the logic
(copy the CASE statements) and delete `dim_wells`. Using `ref('dim_wells')` inside
`well_360` would create a circular or awkward dependency.

---

## Acceptance Criteria

1. `dbt build --select int_well__oda well_360 --full-refresh` completes clean
2. `well_360` has `oda_well_id` populated for all ODA wells — validate:
   ```sql
   SELECT COUNT(*), COUNT(oda_well_id), COUNT(basin_name)
   FROM well_360
   WHERE in_oda = true
   ```
3. `well_360` has `basin_name` populated matching known well locations
4. `is_revenue_generating` count matches expectations vs `dim_wells` output
5. `dbt parse --warn-error --no-partial-parse` passes
6. `grep -r "ref('dim_wells')" models/` returns zero matches after deletion
7. `dbt build --select state:modified+` passes with no errors

---

## Non-Goals

- No SCD2 / history tracking (future sprint)
- No Working Interest / NRI full integration (future sprint)
- No change to `well_360` grain or unique key (`eid`)
- No changes to `well_360__conflicts` or `well_360__data_quality_summary`
  unless they break (update their refs if needed)
