# WellView (Peloton WellView)

## System Overview

Peloton WellView is Formentera's **well master data and drilling operations system**. It tracks well headers, wellbore geometry, directional surveys, casing/tubing/rod strings, completions, zones, drilling jobs, daily reports, costs, stimulations, perforations, reservoir tests, production failures, artificial lift settings, and well integrity. It is the **system of record for well construction and drilling operations**.

- **Vendor:** Peloton
- **Version:** WellView 12.0
- **Snowflake database:** `PELOTON_FORMENTERAOPS_FORMENTERAOPS_WV120`
- **Snowflake schemas:**
  - `FORMENTERAOPS_WV120_DBO` — source-of-truth data tables (only `WVT_WVSYSINTEGRATION` currently in this schema)
  - `FORMENTERAOPS_WV120_CALC` — all other tables (both real data and precalculated rollups; see Schema Gotcha below)
- **Ingestion:** Fivetran (CDC)
- **Soft delete pattern:** `_fivetran_deleted = true`
- **Deduplication:** `qualify 1 = row_number() over (partition by idrec order by _fivetran_synced desc)`
- **Table prefix:** All source tables are `WVT_WV*` (e.g., `WVT_WVWELLHEADER`, `WVT_WVJOB`)

## Core Hierarchy

WellView organizes data around the **Well Header** as the root entity. Every table carries `IDWELL` (string 32) to link back to its well.

```
Well Header [wvWellHeader]                       ← root entity, one per well
  ├── Well Status History [wvWellStatusHistory]
  ├── Well Alias History [wvWellAlias]
  ├── Elevation History [wvElevationHistory]
  ├── Reference Wells [wvRefWells]
  │
  ├── Wellbores [wvWellbore]                     ← one or more per well (sidetracks)
  │    ├── Wellbore Sections [wvWellboreSize]
  │    ├── Key Depths [wvWellboreKeyDepth]
  │    ├── Directional Surveys [wvWellboreDirSurvey]
  │    │    └── Survey Data Points [wvWellboreDirSurveyData]
  │    ├── Formations [wvWellboreFormation]
  │    ├── Statuses [wvWellboreStatus]
  │    └── Reservoirs [wvWellboreReservoir]
  │
  ├── Zones [wvZone]                             ← wellbore contact intervals
  │    ├── Zone Statuses [wvZoneStatus]
  │    ├── Zone Production Allocation [wvZoneAlloc]
  │    └── Zone Production Types [wvZoneProdTypCalc]  (calc)
  │
  ├── Completions [wvCompletion]                 ← physical flow path configurations
  │    ├── Completion Zones [wvCompletionZone]   ← many-to-many with zones
  │    ├── Completion Statuses [wvCompletionStatus]
  │    └── Completion Flow Path Links [wvCompletionLink]
  │
  ├── Casing Strings [wvCas]
  │    └── Casing Components [wvCasComp]
  │         ├── Tally [wvCasCompTally]
  │         ├── Status History [wvCasCompStatus]
  │         └── Extensions: ECP, Liner Hanger, Slotted Liner
  │
  ├── Cement Jobs [wvCement]
  │    └── Stages [wvCementStage]
  │         └── Fluids [wvCementStageFluid]
  │              └── Additives [wvCementStageFluidAdd]
  │
  ├── Tubing Strings [wvTub]
  │    └── Tubing Components [wvTubComp]
  │         ├── Tally [wvTubCompTally]
  │         └── Extensions: Hanger, ESP Intake/Motor/Pump, Packer, Mandrel, SSSV, Sensor, PCP Stator
  │
  ├── Rod Strings [wvRod]
  │    └── Rod Components [wvRodComp]
  │         └── Extensions: PCP Rotor, Polish Rod, Rod Pump
  │
  ├── Other Strings [wvOtherStr] / Other In Hole [wvOtherInHole]
  │
  ├── Wellheads [wvWellhead]
  │    └── Wellhead Components [wvWellheadComp]
  │
  ├── Perforations [wvPerforation]
  │
  ├── Stimulations [wvStim]
  │    └── Stages [wvStimInt]
  │         ├── Fluid Systems [wvStimIntFluid]
  │         │    └── Additives [wvStimIntFluidAdd]
  │         │         └── Compositions [wvStimIntFluidAddComp]
  │         └── Proppants [wvStimIntProp]
  │
  ├── Well Tests [wvWellTestTrans]
  │    ├── Flow Periods [wvWellTestTransFlowPer]
  │    ├── Gauges [wvWellTestTransGauge]
  │    │    └── Gauge Data [wvWellTestTransGaugeData]
  │    ├── Flow Rates [wvWellTestTransRate]
  │    └── Results [wvWellTestTransResult]
  │
  ├── Production Settings [wvProdSetting]
  │    └── Extensions: ESP, Rod Pump, PCP, Gas Lift, Plunger, Flowing
  │
  ├── Production Failures [wvProblem]
  │    └── Details [wvProblemDetail]
  │         └── Analysis [wvProblemDetailAnalysis]
  │
  ├── Fluid Analysis [wvFluidAnalysis]
  │    └── Extensions: Gas, Oil, Water, Hydrocarbon Liquid (each with compositions)
  │
  ├── Jobs [wvJob]                               ← drilling/completion/workover operations
  │    ├── Job Reports (Daily Ops) [wvJobReport]  ← daily reporting hub
  │    │    ├── Daily Costs [wvJobReportCostGen]
  │    │    ├── Daily Recurring Costs [wvJobReportCostRental]
  │    │    ├── Daily Time Log [wvJobReportTimeLogCalc]  (calc)
  │    │    ├── Daily Personnel [wvJobReportPersonnelCount]
  │    │    └── ~55 daily report calc tables (JR* prefix)
  │    ├── AFEs [wvJobAFE]
  │    │    └── AFE Cost Breakdown [wvJobAFECost]
  │    ├── Phases [wvJobProgramPhase]
  │    ├── Rigs [wvJobRig]
  │    │    ├── Crews, Mud Pumps, BOPs, Shakers, Lines, Tanks
  │    │    └── Deep equipment hierarchy (5+ levels)
  │    ├── Drill Strings [wvJobDrillString]
  │    │    └── Components [wvJobDrillStringComp]
  │    ├── Time Logs [wvJobTimeLog]
  │    ├── Interval Problems (NPT) [wvJobIntervalProblem]
  │    ├── Safety Incidents [wvJobSafetyIncident]
  │    ├── Kicks [wvJobKick]
  │    ├── Mud Additives/Checks/Volumes
  │    └── ~40 job-level calc tables (J* prefix)
  │
  ├── Integrity Programs [wvIntegrity]
  │    └── Items [wvIntegrityItem] → Criteria + Values
  │
  ├── Well Barriers [wvWellBarrier]
  │    └── Barrier Types [wvWellBarrierTyp] → Links + Annotations
  │
  ├── Agreements [wvAgreement]
  │    ├── Interest Details [wvAgreementInt]
  │    └── Partners/Contacts/Links/Dates
  │
  └── Polymorphic (attach to any entity):
       ├── Attributes [wvAttributes]
       ├── Attachments [wvAttachment]
       ├── Comments [wvComment]
       ├── Sign Off & QC [wvSignOff]
       └── Locations [wvLocLatLongUTM]
```

**Total data model: ~496 tables** (144 calc, 42 extension, 310 standalone data tables).

## Key Join Patterns

| Relationship | Join Condition |
|---|---|
| Any table → Well Header | `child.idwell = well_header.idwell` |
| Parent → Child (standard) | `parent.idrec = child.idrecparent` |
| Wellbore → Downhole data | `wellbore.idrec = child.idrecwellbore` |
| Completion → Zone | via `wvCompletionZone` junction table |
| Completion → Flow path equipment | via `wvCompletionLink` (links to casing, tubing, etc.) |
| Extension → Parent | Extension shares the same `IDRec` as its parent record |
| Polymorphic → Any entity | `child.tblkeyparent` identifies the parent table + `child.idrecparent` |
| Equipment link → Target | `link.idrecitem` + `link.idrecitemtk` (table key resolves target table) |

### IDWELL — Universal Well Foreign Key

Every table in WellView carries `IDWELL` (string 32), a GUID that links to `wvWellHeader.IDRec`. This is the universal well identifier, equivalent to ProdView's `IDFLOWNET` in terms of scoping.

### IDREC / IDRECPARENT — Standard Parent-Child

`IDRec` (string 32) is the primary key on every table. `IDRecParent` (string 32) links child records to their parent entity within a hierarchy. This is the same pattern as ProdView.

### Extension Tables

42 tables across the data model are **extension tables**. An extension shares the same `IDRec` as its parent — it extends the parent record with type-specific fields rather than creating a new record. Examples:
- `wvCasCompECP` extends `wvCasComp` for external casing packers
- `wvProdSettingRodPump` extends `wvProdSetting` for rod pump configurations
- `wvTubCompPacker` extends `wvTubComp` for packer details

**Join pattern:** `parent.idrec = extension.idrecparent` (1:1 relationship).

### Polymorphic Tables (TblKeyParent)

5 tables in the "Other" domain can attach to **any** entity in WellView:
- `wvAttributes`, `wvAttachment`, `wvComment`, `wvSignOff`, `wvLocLatLongUTM`

They use `TblKeyParent` (string 52) to identify the parent table name and `IDRecParent` to identify the parent record. This is WellView's generic extensibility mechanism.

### Link Tables (Cross-Entity References)

Tables with `*Link` suffix (e.g., `wvCompletionLink`, `wvIntegrityItemLink`, `wvWellBarrierTypLink`) create many-to-many relationships between entities using:
- `IDRecItem` — the referenced record's IDRec
- `IDRecItemTK` (string 26) — the referenced record's table name (polymorphic FK)

## Calc Tables

144 of 496 tables (29%) are **precalculated reporting tables**. These are system-computed rollups, not source-of-truth data.

### Identifying Calc Tables
- Table names end in `Calc` (e.g., `wvWBRigActivityCalc`, `wvJRCostSumCalc`)
- All columns are flagged as `calculated` in the data model
- They live in the `FORMENTERAOPS_WV120_CALC` schema alongside real data tables

### Calc Table Naming Conventions (Operations)
| Prefix | Scope | Example |
|---|---|---|
| `wvJR*Calc` | Daily Report rollups | `wvJRCostSumCalc` (daily cost summary) |
| `wvJPP*Calc` | Phase-level rollups | `wvJPPCostSumCalc` (phase cost summary) |
| `wvJDS*Calc` | Drill String rollups | `wvJDSRigActivityCalc` (drill string rig activity) |
| `wvJ*Calc` | Job-level rollups | `wvJCostSumCalc` (job cost summary) |
| `wvJTL*Calc` | Time Log rollups | `wvJTLRigActivityCalc` (time log rig activity) |
| `wvWB*Calc` | Wellbore rollups | `wvWBRigActivityCalc` (wellbore rig activity) |
| `wvWBF*Calc` | Formation rollups | `wvWBFRigActivityCalc` (formation rig activity) |

### When to Use Calc Tables
- **Future metrics/semantic layer** — pre-aggregated data for dashboards
- **Validation** — cross-check staging model aggregations against system-computed values
- **Performance** — avoid recomputing expensive rollups in dbt when the system already provides them
- **Do not** treat as source-of-truth — they can lag behind real data

## Schema Reference

Detailed column-level schemas are in `context/sources/wellview/`.
Load `_index.yaml` to find the right domain file for your task.

## Existing dbt Models (58 staging + 17 Wiserock)

### Staging Model Coverage by Domain

| Domain | YAML File | Source Tables | Staged | Coverage |
|---|---|---|---|---|
| **General** | `general.yaml` | 5 | 3 | `well_header`, `well_status_history`, `reference_wells` |
| **Wellbore & Surveys** | `wellbore_surveys.yaml` | 24 | 4 | `wellbores`, `wellbore_depths`, `wellbore_directional_surveys`, `wellbore_directional_survey_data` |
| **Casing, Cement & Wellheads** | `casing_cement.yaml` | 23 | 8 | `casing_strings`, `casing_components`, `casing_tally`, `cement_activities`, `cement_stages`, `cement_stage_fluids`, `wellheads`, `wellhead_components` |
| **Geological Evaluations** | `geological_evaluations.yaml` | 21 | 0 | — |
| **Perfs, Stims & Swabs** | `perfs_stims.yaml` | 15 | 8 | `perforations`, `stimulations`, `stimulation_intervals`, `stimulation_fluid_systems`, `stimulation_fluid_additives`, `stimulation_proppant`, `swabs`, `swab_details` |
| **Zones & Completions** | `zones_completions.yaml` | 12 | 1 | `zones` |
| **Tubing, Rods & Equipment** | `tubing_rods_equipment.yaml` | 46 | 8 | `tubing_strings`, `tubing_components`, `tubing_component_mandrels`, `tubing_component_mandrel_inserts`, `tubing_run_tallies`, `rod_strings`, `rod_components`, `other_in_hole_equipment` |
| **Surface Equipment** | `surface_equipment.yaml` | 10 | 0 | — |
| **Reservoir & Equipment Tests** | `reservoir_tests.yaml` | 25 | 3 | `well_tests`, `well_test_flow_periods`, `well_test_results` |
| **Production Ops & Failures** | `production_operations.yaml` | 31 | 1 | `production_failures` |
| **Asset Management** | `asset_management.yaml` | 15 | 0 | — |
| **Integrity & Barriers** | `integrity_barriers.yaml` | 10 | 0 | — |
| **Other** | `other.yaml` | 10 | 0 | — |
| **Operations: Jobs** | `operations_jobs.yaml` | 244 | 21 | `jobs`, `job_reports`, `daily_costs`, `daily_recurring_costs`, `daily_personnel_logs`, `job_time_log`, `job_program_phases`, `job_afe_definitions`, `rigs`, `drill_strings`, `drill_string_components`, `drill_string_drilling_parameters`, `rig_mud_pumps`, `rig_mud_pump_operations`, `rig_mud_pump_checks`, `job_supplies`, `job_supply_amounts`, `mud_additivies`, `mud_checks`, `job_interval_problems`, `drill_bits` |
| **Operations: Tasks** | `operations_tasks.yaml` | 2 | 0 | — |
| **Operations: Inspections** | `operations_inspections.yaml` | 3 | 0 | — |
| **Cross-system** | (in `src_wellview.yml`) | 1 | 1 | `system_integrations` |

**Total: 58 staging models covering 49 unique source tables out of ~496 total.**

### Wiserock Staging Models (17)

Separate staging models in `staging/wellview/wiserock_tables/` that expose WellView data shaped for the Wiserock well analytics application. These overlap with 17 of the 58 primary staging models, referencing the same source tables with Wiserock-specific column selections.

### Source Schema Setup

| dbt Source Name | Snowflake Schema | Tables |
|---|---|---|
| `wellview` | `FORMENTERAOPS_WV120_DBO` | 1 (`WVT_WVSYSINTEGRATION`) |
| `wellview_calcs` | `FORMENTERAOPS_WV120_CALC` | 48+ (all other staged tables) |

**Schema gotcha:** Despite the name, `FORMENTERAOPS_WV120_CALC` contains both real data tables (e.g., `WVT_WVWELLHEADER`, `WVT_WVJOB`) and precalculated rollup tables. The DBO schema only has `WVT_WVSYSINTEGRATION`. This is a Fivetran sync configuration artifact — when adding new staging models, source from `wellview_calcs` unless the table is specifically in DBO.

## Cross-System Integration

WellView connects to ProdView and SiteView via the **System Integration table** (`WVT_WVSYSINTEGRATION` in the DBO schema):

| Product | Links To |
|---|---|
| ProdView | Production volumes, allocations, completions |
| SiteView | SCADA / field data |

**Already staged:** `stg_wellview__system_integrations` (sources from `wellview.WVT_WVSYSINTEGRATION`).

The ProdView intermediate model `int_prodview__well_header` uses this table to link ProdView units to WellView wells.

## User-Defined Fields (Formentera Customizations)

WellView provides generic `UserTxt1-10`, `UserNum1-6`, `UserDtTm1-5`, `UserBoolean1-5` fields on many tables. Key Formentera mappings on Well Header:

| Field | Formentera Usage |
|---|---|
| UserNum1 | WI (Working Interest) |
| UserNum2 | NRI (Net Revenue Interest) |
| UserTxt1-10, UserDtTm1-5 | Various — document in domain YAMLs per table |

*Full user-defined field mappings will be documented in each domain YAML file.*

## Gotchas and Edge Cases

1. **"Well Header" is the root:** Unlike ProdView (which uses Unit → Completion), WellView roots everything at the Well Header level. The `IDWELL` field on every table points to `wvWellHeader.IDRec`.

2. **Schema naming is misleading:** The `FORMENTERAOPS_WV120_CALC` schema contains both real data tables and calculated rollups. Only `WVT_WVSYSINTEGRATION` lives in the `DBO` schema. Always check `src_wellview_calcs.yml` for available tables.

3. **Extension tables share IDRec:** Extension tables do NOT have their own IDRec — they share the parent's IDRec. Join via `parent.idrec = extension.idrecparent` (1:1). Do not expect a unique IDRec on the extension.

4. **Depth convention:** Measured Depth (MD) is the primary depth reference throughout WellView. True Vertical Depth (TVD) columns are always calculated (suffix `TVDCalc`). Depths are stored in meters — convert to feet with `/ 0.3048`.

5. **ProposedOrActual:** Many equipment tables (casing, tubing, rod strings, drill strings) track both planned and as-built configurations via a `ProposedOrActual` field. Filter appropriately when building models.

6. **Operations tree is massive:** The Jobs hierarchy has 249 tables (125 calc). Daily Report (`wvJobReport`) alone has ~55 calc child tables. The operations domain YAMLs are split into 3 files for manageability.

7. **Calc tables vs. calc columns:** Some tables have individual calculated columns (e.g., `wvWellHeader.TDCalc`, `wvWellHeader.CurrentWellTyp1Calc`) that are column-level computations, not to be confused with calc tables (entire tables that are system-computed).

8. **System audit columns:** Every table has `sysCreateDate`, `sysCreateUser`, `sysModDate`, `sysModUser`, `sysLockMe`, `sysLockChildren`, `sysLockDate`, `sysTag`. Use UTC timestamps (`sysCreateDate`, `sysModDate`). Lock fields are informational only.

9. **Table key pattern:** `IDRecItemTK` (string 26) stores a table name, enabling polymorphic foreign keys. Used in link tables and polymorphic tables (Attributes, Attachments, Comments, etc.).

10. **WellView vs. ProdView completions:** WellView `wvCompletion` tracks physical flow path configurations. ProdView `pvUnitComp` tracks producing intervals for allocation. They represent overlapping but distinct concepts — linked via the System Integration table.
