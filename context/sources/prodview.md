# ProdView (Peloton ProdView)

## System Overview

Peloton ProdView is Formentera's **production accounting and surveillance system**. It tracks daily/monthly production volumes, well completions, artificial lift equipment, tank inventories, meter readings, and allocation calculations. It is the **system of record for operated production volumes**.

- **Vendor:** Peloton (now part of Quorum)
- **Version:** ProdView 5.0
- **Snowflake database:** `PELOTON_FORMENTERAOPS_FORMENTERAOPS_PV30`
- **Snowflake schema:** `FORMENTERAOPS_PV30_DBO`
- **Ingestion:** Fivetran (CDC)
- **Soft delete pattern:** `_fivetran_deleted = true`
- **Deduplication:** `qualify 1 = row_number() over (partition by idrec order by _fivetran_synced desc)`
- **Table prefix:** All source tables are `PVT_PV*` (e.g., `PVT_PVUNIT`, `PVT_PVUNITALLOCMONTHDAY`)

## Core Hierarchy

ProdView organizes data in a strict hierarchy. Understanding this is critical to building correct joins.

```
Flow Network (pvFlowNetHeader)
  └── Unit (pvUnit)                     ← "Unit" = well pad / lease / producing entity
       ├── Completion (pvUnitComp)       ← producing interval within a unit
       │    ├── Status (pvUnitCompStatus)
       │    ├── Parameters (pvUnitCompParam)
       │    ├── Production Tests (pvUnitCompTest)
       │    ├── Downtimes (pvUnitCompDownTm)
       │    ├── Artificial Lift (pvUnitCompPump)
       │    │    ├── Rod Pump Config (pvUnitCompPumpRod) — 1:1 extension
       │    │    │    └── Rod Pump Entries (pvUnitCompPumpRodEntry) — daily readings
       │    │    ├── ESP Config (pvUnitCompPumpESP) — 1:1 extension
       │    │    │    └── ESP Entries (pvUnitCompPumpESPEntry) — daily readings
       │    │    ├── PCP Entries (pvUnitCompPumpPCPEntry) — daily readings
       │    │    ├── Jet Pump Entries (pvUnitCompPumpJetEntry) — daily readings
       │    │    └── Plunger Lift Entries (pvUnitCompPumpPlungerEntry) — daily readings
       │    └── Targets (pvUnitCompTarget / pvUnitCompTargetDay)
       ├── Tank (pvUnitTank)
       │    ├── Tank Entries / Readings (pvUnitTankEntry)
       │    ├── Strap Tables (pvUnitTankStrap / pvUnitTankStrapData)
       │    └── Monthly/Daily Volumes (pvUnitTankMonthCalc / pvUnitTankMonthDayCalc)
       ├── Meters (5 types — Liquid, Gas PD, Orifice, Rate, Virtual)
       │    ├── Meter Config (pvUnitMeter{Type})
       │    ├── Meter Factors (pvUnitMeter{Type}Factor) — effective-dated
       │    └── Meter Entries (pvUnitMeter{Type}Entry) — readings
       ├── Nodes (pvUnitNode)
       │    ├── Node Flow Connections (pvUnitNodeFlowTo)
       │    ├── Node Corrections (pvUnitNodeCorr / pvUnitNodeCorrDay)
       │    └── Node Volumes (pvUnitNodeMonthCalc / pvUnitNodeMonthDayCalc)
       ├── Equipment (pvUnitEquip) & Compressors (pvUnitCompressor)
       ├── Allocations — Monthly (pvUnitAllocMonth) / Daily (pvUnitAllocMonthDay)
       ├── Dispositions — Monthly (pvUnitDispMonth) / Daily (pvUnitDispMonthDay)
       └── Remarks (pvUnitRemark)
```

### Key Join Patterns

| Relationship | Join Condition |
|---|---|
| Unit → Completion | `unit.idrec = comp.idrecparent` |
| Completion → Status/Param/Downtime | `comp.idrec = child.idrecparent` AND `comp.idflownet = child.idflownet` |
| Completion → Artificial Lift | `comp.idrec = pump.idrecparent` |
| Artificial Lift → Extension (Rod/ESP) | `pump.idrec = extension.idrecparent` (1:1) |
| Extension → Daily Entries | `extension.idrec = entry.idrecparent` |
| Daily Allocation → Downtime | `alloc.idrecdowntime = downtime.idrec` |
| Daily Allocation → Parameters | `alloc.idrecparam = param.idrec` |
| Daily Allocation → Status | `alloc.idrecstatus = status.idrec` |
| Unit → System Integration | `unit.idrec = integration.idrecparent` AND matching `idflownet` |

**IDRECPARENT pattern:** Most child tables use `IDRECPARENT` to point to their parent. Always also match on `IDFLOWNET` when joining within the flow network hierarchy.

**IDREC:** Every table has a GUID primary key called `IDREC` (string 32). This is the universal PK pattern.

**IDFLOWNET:** Every table within the hierarchy carries the flow network ID. Use this for scoping queries and as a secondary join condition.

## Unit Conversions — Critical

ProdView stores **all measurements in metric/SI units internally**. Every numeric value must be converted to imperial/oilfield units for Formentera's models.

**Use the project macros in `macros/prodview_helpers/prodview_unit_conversions.sql`:**

| From (ProdView Internal) | To (Formentera) | Macro | Division Factor |
|---|---|---|---|
| cubic meters (volume) | barrels (BBL) | `pv_cbm_to_bbl()` | / 0.158987294928 |
| cubic meters (gas) | MCF | `pv_cbm_to_mcf()` | / 28.316846592 |
| meters (length) | inches | `pv_meters_to_inches()` | / 0.0254 |
| meters (depth) | feet | `pv_meters_to_feet()` | / 0.3048 |
| meters (choke) | 64ths of inch | `pv_meters_to_64ths_inch()` | / 0.000396875 |
| kilopascals | PSI | `pv_kpa_to_psi()` | / 6.894757 |
| days (duration) | hours | `pv_days_to_hours()` | / 0.0416666666666667 |
| decimal (0-1) | percent (0-100) | `pv_decimal_to_pct()` | / 0.01 |
| watts | horsepower | `pv_watts_to_hp()` | / 745.6999 |
| joules | MMBTU | `pv_joules_to_mmbtu()` | / 1055055852.62 |
| kg/m³ → API gravity | See formula | N/A | `power(nullif(density, 0), -1) / 7.07409872233005E-06 + -131.5` |
| cbm/day (rate) | BBL/DAY | `pv_cbm_to_bbl_per_day()` | / 0.1589873 |
| cbm/cbm (GOR) | MCF/BBL | `pv_cbm_ratio_to_mcf_per_bbl()` | / 178.107606679035 |

**Exception:** WiseRock-specific staging models (`stg_wiserock__pv_*`) consume raw metric values and do NOT use these macros.

## Existing dbt Models (67 staging + 5 intermediate)

### Staging Models (67)

The staging layer has complete coverage of the core ProdView tables. Key models:

| Model | Source Table | Domain | Notes |
|---|---|---|---|
| `stg_prodview__units` | PVT_PVUNIT | Well master | "Unit" = well/pad. Contains location, identifiers, status. Key fields: Property EID, API 10, Property Number, Combo Curve ID |
| `stg_prodview__completions` | PVT_PVUNITCOMP | Well completions | Producing intervals. Key fields: Well Name, API 10, EID, POP Date, Spud Date, producing formation |
| `stg_prodview__daily_allocations` | PVT_PVUNITALLOCMONTHDAY | Production volumes | **Most important table.** Daily allocated volumes per completion. All volume/NRI/WI conversions happen here |
| `stg_prodview__monthly_allocations` | PVT_PVUNITALLOCMONTH | Production volumes | Monthly rollup of allocations |
| `stg_prodview__completion_downtimes` | PVT_PVUNITCOMPDOWNTM | Production | Downtime events with reason codes |
| `stg_prodview__completion_parameters` | PVT_PVUNITCOMPPARAM | Surveillance | Daily well parameters (pressures, temperatures, choke) |
| `stg_prodview__status` | PVT_PVUNITCOMPSTATUS | Well status | Effective-dated status changes (Active, Shut-in, Abandoned, etc.) |
| `stg_prodview__production_tests` | PVT_PVUNITCOMPTEST | Surveillance | Flow tests (oil/gas/water rates) |
| `stg_prodview__artificial_lift` | PVT_PVUNITCOMPPUMP | Equipment | Pump installations (rod, ESP, PCP, jet, plunger) |
| `stg_prodview__rod_pump_configs` | PVT_PVUNITCOMPPUMPROD | Equipment | Rod pump static specs (1:1 extension of artificial_lift) |
| `stg_prodview__rod_pump_entries` | PVT_PVUNITCOMPPUMPRODENTRY | Equipment | Daily rod pump readings (SPM, stroke length, run time) |
| `stg_prodview__tanks` | PVT_PVUNITTANK | Tanks | Tank master data |
| `stg_prodview__tank_readings` | PVT_PVUNITTANKENTRY | Tanks | Tank gauge readings |
| `stg_prodview__networks` | PVT_PVFLOWNETHEADER | Network | Flow network header (one per Formentera FN — only has 1) |

**Note on column naming:** The existing staging models use **quoted descriptive aliases** (e.g., `"Allocated Gas mcf"`, `"Unit Record ID"`). This is a legacy pattern — these are human-readable display names, not snake_case. New staging models should follow the CLAUDE.md CTE pattern with snake_case.

### Intermediate Models (5 ProdView-specific)

| Model | Purpose |
|---|---|
| `int_prodview__production_volumes` | **Core production fact.** Joins daily_allocations + downtimes + parameters + status. Calculates BOE, net sales, deferred volumes |
| `int_prodview__well_header` | Joins units + completions + system_integrations. Adds SiteView/WellView integration IDs |
| `int_prodview__completion_downtimes` | Enhanced downtime records |
| `int_prodview__production_targets` | Target vs actual comparisons |
| `int_prodview__tank_volumes` | Tank volume rollups |

### WiseRock Staging Models (13)

Separate staging models in `staging/prodview/wiserock_tables/` that expose **raw metric values** for the WiseRock well analytics application. These are NOT converted to imperial units.

## Data Model Domains

The ProdView data model has ~173 tables organized into these domains:

### 1. Asset Master (Unit + Completion)
The foundation. A **Unit** is ProdView's term for a well or producing entity. A **Completion** is a producing interval within a unit. Most Formentera wells have exactly one completion.

**Key identifiers on Unit:**
- `UNITIDPA` → "Property EID" (primary cross-system identifier)
- `UNITIDA` → "API 10"
- `UNITIDB` → "Property Number" (ODA cost center link)
- `UNITIDC` → "Combo Curve ID"
- `LEASE` → Lease Name
- `COSTCENTERIDA` → Cost Center
- `AREA` → "AssetCo" (asset company grouping)
- `FIELD` → "Foreman Area"
- `PLATFORM` → "Route"

**Key identifiers on Completion:**
- `WELLIDC` → "EID" (well entity ID)
- `WELLIDA` → "API 10"
- `WELLIDB` → "Cost Center"
- `WELLLICENSENO` → "API 14" (Texas/Louisiana/Oklahoma variants)
- `COMPLETIONNAME` → Well Name
- `COMPIDB` → "Gas POP ID"
- `USERTXT1` → "BHA Type" (PAGA/SAGA/RPGA — Formentera-specific)
- `USERTXT2` → "RESCAT" (reserve category — Formentera-specific)

### 2. Production Allocation (Daily/Monthly)
The **daily allocation table** (`PVT_PVUNITALLOCMONTHDAY` → `stg_prodview__daily_allocations`) is the most-queried table. It contains:
- Allocated volumes (oil, gas, water, NGL, condensate, sand) per completion per day
- Gathered volumes (pre-allocation)
- New production volumes (post-deductions)
- Disposition breakdowns (sales, fuel, flare, vent, injection)
- Working Interest (WI) and Net Revenue Interest (NRI) percentages
- Downtime hours and operating time
- Inventory (opening, closing, change)
- Cumulative production
- Heat content and density
- FK references to related records (downtime, parameters, status, test, pump entry, facility)

**Grain:** One row per completion per day.

**BOE calculation:** `COALESCE(new_production_hcliq_bbl, 0) + (COALESCE(new_production_gas_mcf, 0) / 6)`

### 3. Surveillance (Parameters, Tests, Status)
Daily operational readings and well status tracking.

- **Parameters** (`pvUnitCompParam`): Pressures (tubing, casing, line, wellhead, bottomhole, injection, shut-in), temperatures, choke size, H2S readings, fluid viscosity, pH
- **Production Tests** (`pvUnitCompTest`): IP tests, flow tests — oil/gas/water volumes over test duration
- **Status** (`pvUnitCompStatus`): Effective-dated status changes. Values include: Active, Shut-in, Abandoned, Suspended, Drilling, Completing, etc.

### 4. Artificial Lift
ProdView supports 5 lift types, each with a config table and daily entry table:

| Lift Type | Config Table | Entry Table | Key Metrics |
|---|---|---|---|
| Rod Pump | `pvUnitCompPumpRod` (1:1 ext) | `pvUnitCompPumpRodEntry` | SPM, stroke length, run time %, volume/day |
| ESP | `pvUnitCompPumpESP` (1:1 ext) | `pvUnitCompPumpESPEntry` | Motor current, frequency, PIP |
| PCP | N/A (parent only) | `pvUnitCompPumpPCPEntry` | Current, rod torque, RPM |
| Jet Pump | N/A (parent only) | `pvUnitCompPumpJetEntry` | RPM, casing pressure, injection pressure |
| Plunger Lift | `pvUnitCompPumpPlunger` | `pvUnitCompPumpPlungerEntry` | Cycles, on/off times |

**Extension table pattern:** Rod Pump and ESP have a **1:1 extension** table linked to the parent artificial lift record via `IDRECPARENT`. The extension holds static configuration; the entry table holds daily readings linked to the extension.

**Seed Records:** ~14% of rod pump entries are "Seed Record" placeholders with null operational data. Filter on `COM != 'Seed Record'` or check for null SPM.

### 5. Tanks & Meters
Tank inventory tracking and 5 meter types (liquid, gas PD, orifice, rate, virtual). Each meter type has config, factors (effective-dated), and entry (readings) tables.

### 6. Flow Network Topology
Nodes and connections defining how production flows from wellhead through measurement points to delivery. Node volumes are calculated during the allocation process.

### 7. Facilities
Multi-unit aggregate reporting. A facility groups units for regulatory reporting (e.g., a central gathering facility receiving production from multiple wells).

### 8. Administrative
Routes (field data collection), partnership agreements, regulatory reporting keys, remarks/comments, approval workflows.

## Cross-System Integration

ProdView connects to other Peloton products via the **System Integration table** (`PVT_PVSYSINTEGRATION`):

| Product | Table Key Filter | Links To |
|---|---|---|
| WellView | `"Product Description" = 'WellView'` | Well master data, drilling, surveys |
| SiteView | `"Product Description" = 'SiteView'` | SCADA / field data |

**Join pattern:** Filter by product + table key, then join on `IDRECPARENT = unit.IDREC` and matching `IDFLOWNET`.

## User-Defined Fields (Formentera Customizations)

ProdView has generic `UserTxt1-5`, `UserNum1-5`, `UserDtTm1-5` fields. Formentera uses these for:

### On Unit (pvUnit)
| Field | Formentera Usage |
|---|---|
| UserTxt2 | Completion Status |
| UserTxt3 | Producing Method |
| UserTxt4 | Stripper Type |
| UserTxt5 | Chemical Provider |
| UserNum1 | Electric Allocation Meter Number |
| UserNum2 | Electric Meter ID |
| UserNum3 | Electric Acct. No. |
| UserNum4 | Electric Vendor No. |
| UserDtTm1 | Stripper Date |
| UserDtTm2 | BHA Change 1 |
| UserDtTm3 | BHA Change 2 |

### On Completion (pvUnitComp)
| Field | Formentera Usage |
|---|---|
| UserTxt1 | BHA Type (PAGA/SAGA/RPGA) |
| UserTxt2 | RESCAT (reserve category) |
| UserTxt3 | Electric Vendor Name |
| UserTxt4 | Electric Meter Name |
| UserTxt5 | Working Interest Partner |
| UserNum1 | Surface Casing |
| UserNum2 | Prod Casing |
| UserNum3 | Prod Liner |
| UserNum4 | Purchaser CTB Lease ID |
| UserNum5 | Purchaser Well Lease ID |
| UserDtTm1 | Spud Date |
| UserDtTm3 | Rig Release Date |

### On Rod Pump Entry (pvUnitCompPumpRodEntry)
| Field | Formentera Usage |
|---|---|
| UserNum1 | Run Time % (decimal 0-1, convert with `pv_decimal_to_pct`) |
| UserTxt1 | Crank position (e.g., "2 of 4") |
| UserTxt2 | Crank rotation direction (CW/CCW) |

## Gotchas and Edge Cases

1. **"Unit" != "Well":** In ProdView, a "Unit" can be a well, a pad, a lease, or any producing entity. Most Formentera units are single wells with one completion, but don't assume 1:1.

2. **Flow Network:** Formentera currently operates a single flow network. All `IDFLOWNET` values should be the same, but always include it in joins for correctness.

3. **Quoted column aliases:** Existing staging models use quoted descriptive names (e.g., `"Allocated Gas mcf"`). This was a deliberate choice to make the models human-readable for field engineers using ProdView data directly. New models being refactored to the CLAUDE.md CTE pattern should use snake_case.

4. **Volume sign conventions:** All production volumes are positive. Deferred/lost volumes are also positive (they represent magnitude of loss). The intermediate model `int_prodview__production_volumes` applies sign flipping for "Gross Downtime BOE" (multiplied by -1).

5. **Density to API gravity:** The conversion formula is non-trivial: `power(nullif(density, 0), -1) / 7.07409872233005E-06 + -131.5`. This converts from kg/m³ to API gravity.

6. **Date fields:** ProdView uses `DtTm` prefix for dates (`DtTmStart`, `DtTmEnd`, `DtTm`). System audit fields use `sysCreateDate`, `sysModDate` (UTC), and `sysCreateDateLocal`, `sysModDateLocal` (local time). Always use the UTC variants.

7. **Lock fields:** Every table has `sysLockMe`, `sysLockChildren`, `sysLockMeUI`, `sysLockChildrenUI`, `sysLockDate`. These are ProdView's record-locking mechanism — informational only, do not filter on them.

8. **Record Tags:** `sysTag` is used for grouping/filtering in the ProdView UI. Informational only.

9. **BOE conversion factor:** 6:1 gas-to-oil (6 MCF gas = 1 BBL oil equivalent). Configured in Calc Settings (`pvCalcSet.BOE` field) but Formentera uses the industry-standard 6:1.

10. **NRI/WI stored as decimals:** Net Revenue Interest and Working Interest are stored as decimals in the allocation table (e.g., 0.75 = 75%). The staging model divides by 0.01 to convert to percentage display format.
