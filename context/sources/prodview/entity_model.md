# ProdView Entity Model

Reference for designing intermediate and mart models from ProdView (Peloton ProdView) data. Describes production entities, their relationships, cross-system resolution, and how they feed into the enterprise analytical model.

## How to Use This Document

- Load when planning new intermediate or mart models that touch ProdView
- Cross-reference with domain YAMLs (`context/sources/prodview/domains/*.yaml`) for table relationships
- Cross-reference with per-table YAMLs (`context/sources/prodview/tables/*.yaml`) for column-level detail
- Entity definitions here are **business-centric** — they describe what the production accounting team cares about, not how ProdView stores it

---

## What ProdView Is (and Is Not)

ProdView is Peloton's **production accounting and surveillance system**. It tracks producing units (well pads, leases, batteries), completions (producing intervals), meter measurements, allocation calculations, and artificial lift operations. ProdView is the system of record for **operated production volumes**.

**Contrast with WellView:** WellView tracks the *physical well* — what the well is structurally, what was drilled, what completion hardware is in the hole. ProdView tracks the *producing entity* — what volumes it makes, how those volumes are measured and allocated, and what the lift equipment is doing day-to-day. A WellView "well" and a ProdView "unit" refer to the same asset from different operational lenses.

---

## Entity Catalog

```
pvFlowNetHeader  (flow network scope — root anchor for all joins)
└── pvUnit  1:many  (producing unit — well pad / lease / battery)
    ├── pvUnitComp  1:many  (completion — MART GRAIN for production)
    │   ├── pvUnitCompDownTm  1:many  (downtime events)
    │   ├── pvUnitCompTest  1:many  (well tests — IP / productivity)
    │   ├── pvUnitCompParam  1:many  (daily surveillance parameters)
    │   ├── pvUnitCompStatus  1:many  (effective-dated status changes)
    │   ├── pvUnitCompGathMonthCalc  1:1  (monthly calc rollup — not raw measurement)
    │   ├── pvUnitCompZone  1:many  (zone / contact interval assignments)
    │   └── pvUnitCompCmngl  1:many  (commingled contribution linkage)
    ├── pvUnitMeter[Type]  1:many  (5 types: Liquid, GasPD, Orifice, Rate, Virtual)
    │   ├── pvUnitMeter[Type]Fact  1:many  (effective-dated factor / config records)
    │   ├── pvUnitMeter[Type]Entry  1:many  (daily readings — time-series grain)
    │   └── pvUnitMeter[Type]ECF  1:1  (+ext,parent: effluent correction factor)
    ├── pvUnitAllocMonth  1:many  (allocation chain — planned/actual monthly)
    │   └── pvUnitAllocMonthDay  1:many  (allocation chain — daily)
    ├── pvUnitDistribMonth  1:many  (distribution chain — downstream monthly)
    │   └── pvUnitDistribMonthDay  1:many  (distribution chain — downstream daily)
    ├── pvUnitBalanceMonthCalc  (balance rollup — alloc in vs. distrib out)
    ├── pvUnitNode  1:many  (flow topology routing nodes)
    │   └── pvUnitNodeFlowTo  1:many  (directed edges — pipeline routing graph)
    ├── pvUnitTank  1:many  (tank inventory tracking)
    │   └── pvUnitTankEntry  1:many  (daily gauge readings)
    └── pvUnitCompPump  1:many  (artificial lift installations)
        ├── pvUnitCompPumpRod  1:1  (+ext: rod pump config)
        │   └── pvUnitCompPumpRodEntry  1:many  (daily rod pump readings)
        └── pvUnitCompPumpESP  1:1  (+ext: ESP config)
            └── pvUnitCompPumpESPEntry  1:many  (daily ESP readings)
```

---

## Core Join Pattern (CRITICAL)

Every table in ProdView shares two universal structural columns:

- `idrec` — GUID primary key (string 32). Present on every single table. The universal PK.
- `idflownet` — flow network identifier. Also present on every table. Serves a dual role.

**The dual role of `idflownet`:**
1. It is a foreign key to `pvFlowNetHeader` — declaring which production network this row belongs to.
2. It is a required scope filter on every parent-to-child join within the hierarchy.

**The compound join pattern used everywhere:**

```sql
-- Correct: always include BOTH idrecparent and idflownet
child.idrecparent = parent.idrec
    AND child.idflownet = parent.idflownet

-- Wrong: omitting idflownet causes cross-network data bleed
child.idrecparent = parent.idrec  -- DO NOT DO THIS
```

Formentera currently operates a single flow network, so all `idflownet` values happen to be identical. Include it in joins anyway — omitting it is semantically wrong and breaks the day a second network is added.

**`idrecparent`** is the universal child-to-parent pointer. It always references the parent table's `idrec`. It must always be paired with a matching `idflownet` predicate.

---

## Entity Details

### pvFlowNetHeader (flow network)

The root scope container for the entire ProdView hierarchy. Every table in ProdView carries `idflownet` referencing this entity.

| Attribute | Description |
|-----------|-------------|
| **Natural key** | `idflownet` (network identifier — also the universal scope key on all child tables) |
| **Role** | Defines the production network boundary — typically a battery, tank battery, lease, or gathering system |
| **Calc settings** | `pvCalcSet` is a 1:1 child holding allocation method, BOE factor, and other network-level calculation parameters |
| **Source table** | `PVT_PVFLOWNETHEADER` |
| **Staging model** | `stg_prodview__networks` |

Formentera has a single flow network. `pvFlowNetHeader` is not a direct staging target for mart models; it provides the `idflownet` scope key that every join requires.

---

### pvUnit (producing unit)

The primary business entity in ProdView. Represents a well, well pad, lease, or other producing entity.

| Attribute | Description |
|-----------|-------------|
| **Natural key** | `idrec` (GUID) |
| **External identifiers** | `UNITIDPA` = Property EID (primary cross-system ID), `UNITIDA` = API 10, `UNITIDB` = Property Number (ODA cost center link), `UNITIDC` = Combo Curve ID |
| **Location fields** | `LEASE` (Lease Name), `FIELD` (Foreman Area), `AREA` (AssetCo grouping), `PLATFORM` (Route), `COSTCENTERIDA` (Cost Center) |
| **Source table** | `PVT_PVUNIT` |
| **Staging model** | `stg_prodview__units` |

**What "Unit" means:** In ProdView, a "Unit" is not necessarily a single wellbore. It can be a well, a pad, a lease aggregate, or any producing entity ProdView tracks. Most Formentera units are single wells with one completion, but do not assume 1:1. The physical wellbore is WellView's domain; pvUnit is the producing entity.

**EID join to well_360:** The canonical cross-system join is `pvUnit.idrec → well_360.prodview_unit_id`. See the EID Resolution section below.

---

### pvUnitComp (completion)

A producing interval within a pvUnit. **This is the mart grain for all production volume facts.**

| Attribute | Description |
|-----------|-------------|
| **Natural key** | `idrec` (GUID) |
| **Parent** | `pvUnit.idrec` via `idrecparent` |
| **Join condition** | `pvUnitComp.idrecparent = pvUnit.idrec AND pvUnitComp.idflownet = pvUnit.idflownet` |
| **External identifiers** | `WELLIDC` = EID, `WELLIDA` = API 10, `WELLIDB` = Cost Center, `COMPLETIONNAME` = Well Name |
| **Key dates** | `DtTmStart` = POP date, `UserDtTm1` = Spud date (Formentera custom field) |
| **Formentera custom fields** | `USERTXT1` = BHA Type (PAGA/SAGA/RPGA), `USERTXT2` = RESCAT (reserve category) |
| **Source table** | `PVT_PVUNITCOMP` |
| **Staging model** | `stg_prodview__completions` |

**Mart design note:** When querying pvUnit to select only completions (excluding facilities and externals), filter `unit_type = 'pvunitcomp'`. Every production volume fact is at this grain: one row per completion per day in the daily allocation table.

pvUnitComp is ProdView's equivalent of WellView's completion domain — but where WellView tracks the downhole hardware, pvUnitComp tracks the producing interval for volume accounting purposes.

---

### pvUnitCompDownTm (downtime events)

Downtime events on a completion, recording when and why a well was not producing at full capacity.

| Attribute | Description |
|-----------|-------------|
| **Natural key** | `idrec` (GUID) |
| **Parent** | `pvUnitComp.idrec` |
| **Join condition** | `pvUnitCompDownTm.idrecparent = pvUnitComp.idrec AND pvUnitCompDownTm.idflownet = pvUnitComp.idflownet` |
| **Primary metric** | `DowntimePct` — downtime percentage per completion per day |
| **Source table** | `PVT_PVUNITCOMPDOWNTM` |
| **Staging model** | `stg_prodview__completion_downtimes` |

Downtime records are FK-referenced from the daily allocation table (`pvUnitAllocMonthDay.idrecdowntime = pvUnitCompDownTm.idrec`), enabling direct downtime attribution on allocation rows without a separate join.

---

### pvUnitCompTest (well tests)

Productivity tests (IP tests, flow tests) measuring well performance at a point in time.

| Attribute | Description |
|-----------|-------------|
| **Natural key** | `idrec` (GUID) |
| **Parent** | `pvUnitComp.idrec` |
| **Join condition** | `pvUnitCompTest.idrecparent = pvUnitComp.idrec AND pvUnitCompTest.idflownet = pvUnitComp.idflownet` |
| **Key metrics** | Oil/gas/water volumes over test duration, productivity index, injection ratio |
| **Source table** | `PVT_PVUNITCOMPTEST` |
| **Staging model** | `stg_prodview__completion_tests` |

**Mart design note:** Use the latest test per completion (highest `DtTm`) for current productivity attributes. Test history is useful for decline analysis.

---

### pvUnitCompParam (completion parameters)

Daily operational surveillance readings: pressures, temperatures, choke size, fluid properties.

| Attribute | Description |
|-----------|-------------|
| **Natural key** | `idrec` (GUID) |
| **Parent** | `pvUnitComp.idrec` |
| **Join condition** | `pvUnitCompParam.idrecparent = pvUnitComp.idrec AND pvUnitCompParam.idflownet = pvUnitComp.idflownet` |
| **Key metrics** | Tubing pressure, casing pressure, line pressure, wellhead pressure, bottomhole pressure, injection pressure, choke size (meters → 64ths of inch via `pv_meters_to_64ths_inch()`), temperatures, H2S |
| **Source table** | `PVT_PVUNITCOMPPARAM` |
| **Staging model** | `stg_prodview__completion_parameters` |

Parameter records are also FK-referenced from daily allocation rows (`pvUnitAllocMonthDay.idrecparam`), so the most recent parameter reading for a given day can be resolved without a separate join.

---

### pvUnitMeter[Type] (meters — 5 types)

ProdView supports five meter types, each following the same three-level hierarchy: Header → Fact → Entry.

| Type | Header Table | Fact Table | Entry Table | Measures |
|------|-------------|------------|-------------|---------|
| Liquid | `pvUnitMeterLiquid` | `pvUnitMeterLiquidFact` | `pvUnitMeterLiquidEntry` | Oil/water volumes |
| GasPD | `pvUnitMeterPDGas` | `pvUnitMeterPDGasFact` | `pvUnitMeterPDGasEntry` | Gas positive displacement |
| Orifice | `pvUnitMeterOrifice` | `pvUnitMeterOrificeHeatFact` | `pvUnitMeterOrificeEntry` | Gas orifice flow |
| Rate | `pvUnitMeterRate` | N/A | N/A | Rate / engineering estimates |
| Virtual | `pvUnitMeterVirtual` | N/A | `pvUnitMeterVirtualEntry` | Calculated multi-product entries |

**Join pattern (Header → Entry):**

```sql
-- Header to Fact (effective-dated config records)
pvUnitMeter[Type]Fact.idrecparent = pvUnitMeter[Type].idrec
    AND pvUnitMeter[Type]Fact.idflownet = pvUnitMeter[Type].idflownet

-- Header to Entry (daily readings)
pvUnitMeter[Type]Entry.idrecparent = pvUnitMeter[Type].idrec
    AND pvUnitMeter[Type]Entry.idflownet = pvUnitMeter[Type].idflownet
```

**Extension tables (+ext,parent):** GasPD and Orifice meters have 1:1 extension tables for effluent correction factors (`pvUnitMeterPDGasECF`, `pvUnitMeterOrificeECF`). These extend the parent record with additional columns — they are NOT 1:many child rows. Orifice also has `pvUnitMeterOrificeRange` (spring ranges) and `pvUnitMeterOrificeEntry` as 1:1 extensions.

**Key design notes:**
- A single pvUnit can have meters of multiple types active simultaneously — do not assume one meter per unit.
- The grain hierarchy is: Entry (time-series daily readings) → Fact (effective-dated configuration) → Header (instrument-level config).
- Heat factor tables (`pvUnitMeterPDGasHeatFact`, `pvUnitMeterOrificeHeatFact`) are separate from volume factor tables — both are children of the meter header via `idrecparent`.
- **Source tables:** `PVT_PVUNITMETER{TYPE}` / `PVT_PVUNITMETER{TYPE}ENTRY` (uppercase in Snowflake).

---

### Allocation and Distribution Chains

Two parallel but distinct chains track how production volumes are accounted for:

**Allocation chain** — planned and actual production allocation per unit:

```
pvUnit
  └── pvUnitAllocMonth  (monthly grain — one row per unit per calendar month)
        └── pvUnitAllocMonthDay  (daily grain — one row per unit per day within month)
```

**Distribution chain** — downstream product distribution:

```
pvUnit
  └── pvUnitDistribMonth  (monthly grain)
        └── pvUnitDistribMonthDay  (daily grain)
```

**Join pattern (same for both chains):**

```sql
-- Monthly header
pvUnitAllocMonth.idrecparent = pvUnit.idrec
    AND pvUnitAllocMonth.idflownet = pvUnit.idflownet

-- Daily detail
pvUnitAllocMonthDay.idrecparent = pvUnitAllocMonth.idrec
    AND pvUnitAllocMonthDay.idflownet = pvUnitAllocMonth.idflownet
```

**Do not mix the two chains.** Allocation = production entering the accounting system. Distribution = product leaving toward a purchaser or end use. They are separate business concepts.

**pvUnitBalanceMonthCalc** is a calculated rollup (not raw measurement) representing the balance between allocated in and distributed out. Use for balance/imbalance checks, not as a production volume source.

The **completion-level allocation tables** (`pvUnitCompAllocCalc` and `pvUnitCompAllocMonthCalc`) are the most-queried tables in ProdView. They hold allocated volumes per completion per day (oil, gas, water, NGL, condensate), WI/NRI percentages, disposition breakdowns (sales, fuel, flare, vent, injection), downtime hours, inventory, and cumulative production. The daily allocation table is the source for `stg_prodview__daily_allocations` and ultimately `int_prodview__production_volumes`.

---

## EID Resolution Strategy

Joining ProdView data to the canonical well dimension (`well_360`) requires a two-step match:

**Step 1 — Primary join (prefer this):**

```sql
prodview_unit.idrec = well_360.prodview_unit_id
```

The `prodview_unit_id` on `well_360` is populated from `pvUnit.UNITIDPA` (Property EID). This is the most reliable match.

**Step 2 — Fallback (for wells missing prodview_unit_id):**

```sql
prodview_unit.api_10 = well_360.api_10
```

Use `COALESCE(step_1_eid, step_2_eid)` to produce the resolved EID, and set an `is_eid_unresolved` boolean flag when neither join succeeds.

**Filter requirement:** Filter `unit_type = 'pvunitcomp'` on the pvUnit table before joining to well_360. This excludes facilities and external entities (~3K rows) that do not correspond to individual wells.

**Expected match rates:**
- ~81.5% resolve via the two-step join
- ~18.5% unresolved — includes non-operated wells, injection/SWD wells, and operated gas wells (FP Griffin migration gap where EIDs were not added to ProdView)

**Do not drop unmatched rows.** Include all `pvunitcomp` rows in production facts with `is_eid_unresolved = true` for unmatched rows. The well_360 spine gap is a data quality issue to fix separately; the production volumes are real.

---

## Unit Conversions

ProdView stores all measurements internally in **metric/SI units**. Every numeric value must be converted to imperial/oilfield units in staging models. Use the project macros in `macros/prodview_helpers/prodview_unit_conversions.sql`.

| ProdView Internal Unit | Formentera Target Unit | Macro |
|------------------------|------------------------|-------|
| cubic meters (liquid volume) | barrels (BBL) | `pv_cbm_to_bbl()` |
| cubic meters (gas volume) | MCF | `pv_cbm_to_mcf()` |
| cubic meters/day (liquid rate) | BBL/day | `pv_cbm_to_bbl_per_day()` |
| cbm/cbm (gas-oil ratio) | MCF/BBL | `pv_cbm_ratio_to_mcf_per_bbl()` |
| kilopascals | PSI | `pv_kpa_to_psi()` |
| meters (depth / length) | feet | `pv_meters_to_feet()` |
| meters (choke size) | 64ths of inch | `pv_meters_to_64ths_inch()` |
| meters (pipe diameter) | inches | `pv_meters_to_inches()` |
| days | hours | `pv_days_to_hours()` |
| decimal (0–1) | percent (0–100) | `pv_decimal_to_pct()` |
| watts | horsepower | `pv_watts_to_hp()` |
| joules | MMBTU | `pv_joules_to_mmbtu()` |

**Special case — API gravity:** Density (kg/m³) to API gravity uses a non-trivial formula, not a macro:

```sql
power(nullif(density_kg_m3, 0), -1) / 7.07409872233005E-06 + -131.5
```

**WI/NRI sign convention:** Working interest and net revenue interest are stored as decimals (0.75 = 75%). The staging model divides by 0.01 to convert to percentage display format.

**Volume sign convention:** All production volumes are positive. Deferred/lost volumes are also positive (representing the magnitude of loss). Sign flipping (e.g., Gross Downtime BOE × -1) is applied in the intermediate model `int_prodview__production_volumes`, not in staging.

**WiseRock exception:** Staging models under `staging/prodview/wiserock_tables/` consume raw metric values and do NOT apply these macros. Do not add conversion macros to WiseRock-prefixed models.

---

## Sprint Coverage

### Sprint 1 (this document)

Fully documented domains with relationships and key_patterns:
- `completions` — see `context/sources/prodview/domains/completions.yaml`
- `allocations` — see `context/sources/prodview/domains/allocations.yaml`
- `meters` — see `context/sources/prodview/domains/meters.yaml`
- `flow_network` — see `context/sources/prodview/domains/flow_network.yaml`

### Sprint 2 (TBD)

Relationship details and key_patterns not yet documented for:
- `artificial_lift` — lift method tracking (ESP, rod pump, PCP, jet pump, plunger)
- `facilities` — surface facility equipment and aggregate reporting (multi-unit regulatory reporting)
- `tanks` — tank gauge measurements, strap tables, and inventory calculations
- `routes` — field data collection route assignments
- `admin` — partnership agreements, regulatory reporting keys, remarks, tickets (likely not needed for mart modeling)

See individual domain YAML files in `context/sources/prodview/domains/` for table catalogs.
