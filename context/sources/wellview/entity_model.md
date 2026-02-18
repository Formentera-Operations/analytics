# WellView Entity Model

Reference for designing intermediate and mart models from WellView data. Describes business entities, their relationships, cross-system resolution, and how they feed into an enterprise-wide analytical model.

## How to Use This Document

- Load when planning new intermediate or mart models that touch WellView
- Cross-reference with domain YAMLs (`context/sources/wellview/*.yaml`) for column-level detail
- Cross-reference with `calc_table_blueprint.md` for metric definitions and calc table mapping
- Entity definitions here are **business-centric** — they describe what the business cares about, not how WellView stores data

---

## Two Core Models

WellView data organizes into two fundamental entity models that answer different questions:

| Model | Question It Answers | Nature | Volume |
|-------|---------------------|--------|--------|
| **Physical Well** | "What IS the well?" | Asset-centric, slowly changing | ~500 wells, deep hierarchy |
| **Well Work** | "What was DONE to the well?" | Event-centric, time-series | Thousands of daily records |

These two models share a single root entity — the **Well** — but diverge from there. Physical Well describes the well as a constructed, configured asset. Well Work describes the operational activities performed on it.

### Validated Against Real Data

Entity model validated using **HOOEY-STX-UNIT N731H** (EID `109181`, WellView ID `FF3A6D0E6DB24DF1A7CC596EA4119A95`) — a horizontal Frio County, TX well drilled April–June 2025, completed July–August 2025, in all 4 source systems (ODA, Combo Curve, WellView, ProdView).

**Physical Well entities confirmed:** 1 wellbore (Original Hole, horizontal, 19,777 ft TD), 4 casing strings (Conductor 20" → Surface 13⅜" → Intermediate 9⅝" → Production 5½"), 512 perforations (1,020 shots, 10,142–19,656 ft), 1 sand frac stimulation (64 stages, 512 clusters, $4.7M, Liberty Energy), 0 agreements (confirming land/mineral data gap).

**Well Work entities confirmed:** 6 jobs spanning 4 categories over 10 months (Drilling $5.0M, Completion $4.7M, Flowback $683K, Facilities $1.2M + $140K, Swab $15K), 156 daily reports, 14 phases (9 with actuals), 2,123 daily cost line items totaling ~$11.8M, 3 NPT events.

**Key findings:**
- Two-model split maps cleanly to real data — no entity orphans or ambiguous placement
- Stimulation confirmed as dual-membership entity (physical completion + operational cost/time)
- Perfs and stim are tightly coupled: 512 perfs = 512 clusters = 64 stages × 8 clusters/stage
- Multiple concurrent jobs (Drilling + Facilities overlapped May–June 2025)
- Facilities and Well Servicing represent distinct lifecycle phases beyond drilling/completion
- AFE vs field estimate variance trackable per job (Facilities overran: $776K AFE → $1.2M actual)

---

## Model 1: Physical Well — "What the Well IS"

The well as a physical asset: its identity, its downhole configuration, the rock it penetrates, and the equipment installed in it. Changes slowly over the well's lifecycle (decades).

### Entity Catalog

```
Well
  │
  ├── Wellbore                    1:N (sidetracks, laterals)
  │     ├── Wellbore Section      1:N (hole sizes drilled)
  │     ├── Key Depth             1:N (casing shoe, KOP, landing point, TD)
  │     ├── Directional Survey    1:N (survey runs)
  │     │     └── Survey Station  1:N (MD/incl/azimuth data points)
  │     ├── Formation             1:N (geological layers penetrated)
  │     └── Reservoir             1:N (reservoir properties at wellbore)
  │
  ├── Zone                        1:N (reservoir contact intervals)
  │     ├── Zone Status           1:N (open/shut-in/abandoned history)
  │     └── Zone Allocation       1:N (production allocation factors)
  │
  ├── Completion                  1:N (flow path configurations)
  │     ├── Completion Zone       M:N junction (which zones feed this completion)
  │     ├── Completion Status     1:N (active/suspended history)
  │     └── Completion Link       1:N (links to casing/tubing/equipment)
  │
  ├── Casing String               1:N (structural steel)
  │     └── Casing Component      1:N (joints, shoes, float collars)
  │
  ├── Cement Job                  1:N (cement operations)
  │     └── Cement Stage          1:N (primary, squeeze, remedial)
  │
  ├── Tubing String               1:N (production conduit)
  │     └── Tubing Component      1:N (joints, packers, ESPs, mandrels, SSSVs)
  │
  ├── Rod String                  1:N (rod pump systems)
  │     └── Rod Component         1:N (rods, guides, pump)
  │
  ├── Perforation                 1:N (holes in casing)
  │
  ├── Wellhead                    1:1 (surface termination)
  │     └── Wellhead Component    1:N (spools, valves, hangers)
  │
  ├── Surface Equipment           1:N (pumping units, prime movers)
  │
  ├── Integrity Program           1:N (well integrity management)
  │     └── Integrity Item        1:N (barrier elements, test criteria)
  │
  ├── Well Status History         1:N (status changes over time)
  │
  └── Operator History            1:N (operator changes over time)
```

### Entity Details

#### Well (root entity)

The top-level identity for a physical well location.

| Attribute | Description | Source |
|-----------|-------------|--------|
| **Natural key** | `IDWELL` (GUID) — WellView's internal well ID |
| **Business keys** | EID (6-char Formentera ID), API-10, Cost Center |
| **Surrogate key** | `well_sk` via `generate_surrogate_key(['id_well'])` |
| **Core attributes** | Well name, operator, basin, field, state/county, lat/long, configuration type |
| **Lifecycle dates** | Permit, spud, rig release, first production, last production, abandonment |
| **Ownership snapshot** | WI (UserNum1), NRI total (UserNum2), NRI-WI only (UserNum3), override decimal (UserNum4), mineral royalty decimal (UserNum5) |
| **Source tables** | `wvWellHeader`, `wvWellStatusHistory`, `wvWellAlias`, `wvElevationHistory` |
| **Staging models** | `stg_wellview__well_header`, `stg_wellview__well_status_history` |
| **Existing marts** | `well_360` (golden record across all systems) |

**Golden record note:** The Well entity is resolved cross-system in `well_360.sql` using a spine + COALESCE priority pattern. WellView is authoritative for drilling/operations attributes (spud date, permit date, well configuration). See [Cross-System Resolution](#cross-system-resolution) below.

#### Wellbore

A distinct drilled hole. The original hole is the primary wellbore; sidetracks and laterals are additional wellbores on the same well.

| Attribute | Description |
|-----------|-------------|
| **Natural key** | `IDRec` on `wvWellbore` |
| **Parent** | Well (via `IDWELL`) |
| **Core attributes** | Wellbore name, number, type, status, proposed/actual flag |
| **Depth attributes** | MD top/bottom, TVD, kick-off point, landing point |
| **Source tables** | `wvWellbore`, `wvWellboreSize`, `wvWellboreKeyDepth`, `wvWellboreStatus` |
| **Staging models** | `stg_wellview__wellbores`, `stg_wellview__wellbore_depths` |
| **Existing marts** | `dim_wellbore` |

#### Zone

A reservoir contact interval — the depth range where the wellbore intersects a producing formation. Zones are the bridge between "where we drilled" and "what we produce."

**Validated with portfolio data (3,055 zones across 1,791 wells):** Zone usage differs fundamentally by well configuration:

| Well Type | Wells w/ Zones | Zones/Well | Status Populated | Prod Dates | Usage Pattern |
|-----------|---------------|------------|-----------------|------------|---------------|
| **Horizontal** | 1,234 | ~1.1 | 3% | <1% | Target formation label only |
| **Vertical** | 161 | ~2.9 | 15% | 6% | Producing interval management |
| **Legacy (null config)** | 365 | ~3.1 | 12% | 17% | Richest data (LA gas wells) |

- **Horizontal wells** use zones as a simple target formation tag — one zone per well, sparse metadata, no status or production dates. The "producing interval" concept for horizontals lives in **Stimulation** (stage/cluster geometry defines what's producing).
- **Vertical wells** use zones as actual producing interval management — multiple formations per well (e.g., J.B. Tubb: 15 zones from Rustler @ 408 ft through Ellenburger @ 6,000 ft), with status tracking (Flowing, Abandoned, Gas Lift) and production date history.
- **2025 wells have zero zones** — zone definition is a backfill/catch-up process, not part of active drilling workflows.
- **HOOEY N731H:** 0 zones (horizontal, recently completed — consistent with pattern).

| Attribute | Description |
|-----------|-------------|
| **Natural key** | `IDRec` on `wvZone` |
| **Parent** | Well (via `IDWELL`), associated with Wellbore |
| **Core attributes** | Zone name, formation, depth top/bottom, status, production type |
| **Source tables** | `wvZone`, `wvZoneStatus`, `wvZoneAlloc`, calc: `wvZoneProdTypCalc`, `wvZoneProdTypDataCalc`, `wvZoneFormationCalc` |
| **Staging models** | `stg_wellview__zones` |

#### Completion

A flow path configuration — the combination of zones, tubing, and surface equipment that allows production. A well can have multiple completions (dual completions, recompletions over time).

**Validated with portfolio data (23 completions across ~18 wells):** Completions are extremely sparse in WellView — only 23 records total. Most have empty metadata (no status, no completion code). The richest example is F N Bullock 8 with 6 completions spanning 1978–2007, showing a classic vertical well recompletion lifecycle. For Formentera's current horizontal well portfolio, the WellView Completion entity is largely unused — "completion" for horizontals is effectively defined by the **Stimulation** entity (stage/cluster geometry) plus the **Perforation** entity (shot intervals).

**Implication for mart design:** A `dim_completion` mart would primarily serve vertical/legacy wells. For horizontals, the "completion configuration" dimension should source from stimulation + perforation data rather than WellView's completion tables.

| Attribute | Description |
|-----------|-------------|
| **Natural key** | `IDRec` on `wvCompletion` |
| **Parent** | Well (via `IDWELL`) |
| **Key relationships** | M:N with Zones (via `wvCompletionZone`), links to Casing/Tubing (via `wvCompletionLink`) |
| **Source tables** | `wvCompletion`, `wvCompletionZone`, `wvCompletionStatus`, `wvCompletionLink` |
| **Staging models** | Not yet staged |
| **Volume** | Only 23 records — very low priority for staging |

**Cross-system note:** WellView completions describe *physical flow path configurations*. ProdView completions (`pvUnitComp`) describe *producing intervals for volume allocation*. They're related but not identical — linked via the System Integration table. For Formentera's horizontal wells, ProdView is the more authoritative completion source.

#### Downhole Equipment (Casing, Tubing, Rods, Perforations)

These are separate entities in WellView but share a common pattern: a **string** (the assembly) containing **components** (individual items), often with **extension tables** for type-specific attributes (ESP, packer, mandrel, etc.).

**Validated with HOOEY N731H:**
- **Casing:** 4 strings showing standard nested architecture — Conductor 20" @ 108 ft → Surface 13⅜" J-55 @ 3,486 ft → Intermediate 9⅝" P-110 @ 9,367 ft → Production 5½" RYS-110 @ 19,754 ft. All actual (not proposed).
- **Perforations:** 512 perfs, 1,020 total shots over 10,142–19,656 ft interval. Tightly coupled with stimulation stages (512 perfs = 512 clusters).

**Portfolio-wide equipment volumes:**

| Entity | Total Records | Unique Wells | Avg/Well | Currently In-Hole |
|--------|--------------|-------------|----------|-------------------|
| Rod Strings | 7,629 | 2,015 | 3.8 | 2,071 |
| Tubing Strings | 11,562 | 3,984 | 2.9 | 4,465 |
| Rod Components | 64,598 | 2,007 | 32.2 | — |
| Tubing Components | 110,676 | 3,959 | 28.0 | — |

**Artificial lift type inference from equipment data:** WellView does NOT track artificial lift type as a well attribute. It must be inferred from equipment entities:

| Inferred Lift Type | Well Count | Inference Rule |
|--------------------|-----------|----------------|
| Rod Pump | 1,886 | Has rod strings + tubing, no ESP tubing |
| ESP | 73 | Has tubing with `description ilike '%ESP%'` |
| Non-Rod (Flowing/Other) | 2,025 | Has tubing but no rod strings |

**Ground-truth: JB Tubb AC 1 #504 (Rod Pump, well_id `10FD428747D44DC89D66AD52F857DCB6`):**
- Vertical well, spud 2022-09-02
- 8 rod string runs (Nov 2022 – present, current in-hole since Dec 2025), ~200-day replacement cycle
- Rod strings: FBG (fiberglass) grade, 1.25" OD, ~4,300 ft set depth
- Typical rod string BOM (bottom-up): Tubing Pump Plunger (ENDURANCE 2.25" x 6') → Lift Sub → Sinker Bars → Shear Tool 26K → ON/OFF Tool → Sucker Rods (KD grade, Norris Grade 40) → Fiberglass Sucker Rods (FBG, Norris) → Pony Rods → Polished Rod (SPM, Norris Sprayloy)
- 8 matching tubing strings: J-55 grade, 2.875" OD, with dump valve, mud joints, desander, seating nipple, nylon tubing, TAC (tubing anchor/catcher)
- Equipment tracks full run/pull lifecycle with duration calculations

**Ground-truth: KING 59 1 (ESP, well_id `38C13B19346F40B2B108201739EF56C1`):**
- Vertical well, spud 2014-04-28
- 5 ESP tubing string runs (Dec 2014 – present, current in-hole since Jun 2017), 0 rod strings (correct — ESPs don't use rods)
- Tubing strings: L-80 grade, 2.875" OD, explicitly labeled "Tubing - ESP", ~6,200 ft set depth
- Typical ESP string BOM (bottom-up): ESP Pressure Sensor (Summit M5-125) → ESP Motor (Summit 456 Series 120HP 1762V LT) → 2× ESP Seal Assembly (Summit 400 Series Modular Seal BPBSL) → ESP Intake (Summit 400 Series) → ESP Gas Separator (Summit 400 Series) → 3× ESP Pump (Summit 400 Series TD-650) → ESP Discharge (400 Series FPHVDIS4) → Tubing Pup Joint → Seat Nipple → 185 joints Tubing (L-80, T&C Upset) → Tubing Hanger (INNOVEX)
- All ESP components carry serial numbers and manufacturer — full BOM traceability
- Pull reasons tracked: "Pulled due to failure." — enables MTBF (mean time between failures) analysis
- 3 failures in first 3 runs (~28, 204, 200 days), then improved to 463 days on 4th run

**Production Settings (`WVT_WVPRODSETTING`) — Canonical Lift Type Source (sparse):**

WellView DOES have a dedicated artificial lift type field (`PRODMETHTYP` on `WVT_WVPRODSETTING`), but it's very sparsely populated:

| Lift Type (PRODMETHTYP) | Wells | Detail Values (PRODMETHDETAIL) |
|-------------------------|-------|-------------------------------|
| Rod Pump (`wvprodsettingrodpump`) | 67 | PU-ELC-AUTO, PU-WH CMP, PU-GAS-AUTO, PU-GAS-MAN |
| ESP (`wvProdSettingESP`) | 27 | VSD |
| Gas Lift (`wvprodsettinggaslift`) | 200+ | GL, Continuous, Annulus & Tubing |
| Plunger (`wvprodsettingplunger`) | 206 | PL, PW, CONV PL-Diff, AUTO PL |
| Flowing (`wvprodsettingflow`) | 106 | FL, FW, FC |
| **NULL** | **2,114** | 19,115 records with no type assigned |

Ground-truth validation:
- **KING 59 1** (ESP): 8 production setting records with `setting_objective = 'ESP pump'`, but `PRODMETHTYP` is NULL
- **JB Tubb AC 1 #504** (Rod Pump): Zero production setting records at all

This means `PRODMETHTYP` covers only ~670 wells vs ~3,984 with equipment data. The `dim_well_equipment` mart should use a **COALESCE priority pattern**: (1) `PRODMETHTYP` when populated, (2) equipment inference from rods/tubing, (3) `setting_objective` as tertiary signal.

**Key equipment modeling insights:**
1. Artificial lift type has TWO sources: `PRODMETHTYP` (authoritative but sparse) and equipment inference (broad coverage) — COALESCE priority pattern needed
2. Rod pump wells have paired rod + tubing string lifecycles; ESP wells have tubing-only lifecycles
3. Component-level data enables failure analysis (MTBF), BOM tracking, and vendor performance
4. Tubing `description` field is the primary lift type discriminator ("Tubing - ESP" vs "Tubing" / "Tubing - Production")
5. Extension tables (`WVT_WVPRODSETTINGESP`, `WVT_WVPRODSETTINGRODPUMP`, etc.) are essentially empty — do not stage

| Entity | Parent | Key Attributes | Staging Coverage |
|--------|--------|----------------|------------------|
| Casing String | Well | Size, weight, grade, depth range | Staged (8 models) |
| Tubing String | Well | Size, weight, grade, depth range | Staged (5 models) |
| Rod String | Well | Size, grade, depth range | Staged (2 models) |
| Perforation | Well | Depth top/bottom, shot density, date | Staged (1 model) |
| Wellhead | Well | Type, components, pressure ratings | Staged (2 models) |

#### Directional Survey

The 3D trajectory of a wellbore, measured at discrete stations.

| Attribute | Description |
|-----------|-------------|
| **Parent** | Wellbore |
| **Grain** | Survey run → survey stations (MD, inclination, azimuth) |
| **Calc tables** | `wvWCompDirSurveyCalc` (composite actual), `wvWCompDirSurveyPropCalc` (composite proposed) |
| **Staging models** | `stg_wellview__wellbore_directional_surveys`, `stg_wellview__wellbore_directional_survey_data` |

---

## Model 2: Well Work — "What's DONE to the Well"

Operational activities performed on a well: drilling, completing, working over, stimulating. Event-driven, time-series, high-volume. This is where the 127 operations calc tables live.

### Entity Catalog

```
Well
  │
  ├── Job                         1:N (drilling/completion/workover programs)
  │     │
  │     ├── Daily Report          1:N (what happened each day)
  │     │     ├── Cost            1:N (money spent that day)
  │     │     ├── Time Log        1:N (time accounting by activity code)
  │     │     ├── NPT Event       1:N (non-productive time incidents)
  │     │     ├── Safety Event    1:N (incidents and checks)
  │     │     ├── Personnel       1:N (headcount by company/role)
  │     │     ├── Mud Check       1:N (mud property measurements)
  │     │     └── Fluids          1:N (lease/well fluid volumes)
  │     │
  │     ├── Phase                 1:N (planned program stages)
  │     │
  │     ├── AFE                   1:N (authorization for expenditure)
  │     │     └── AFE Cost Line   1:N (budgeted cost breakdown)
  │     │
  │     ├── Rig                   1:1 (rig assignment for this job)
  │     │     ├── Rig Crew        1:N
  │     │     ├── Mud Pump        1:N
  │     │     └── BOP / Equipment 1:N
  │     │
  │     ├── Drill String / BHA    1:N (tools used during drilling)
  │     │     └── DS Component    1:N (individual BHA tools)
  │     │
  │     ├── Time Log Entry        1:N (job-level time records)
  │     │
  │     ├── Interval Problem      1:N (NPT at job level)
  │     │
  │     ├── Safety Incident       1:N (HSE events)
  │     │
  │     ├── Kick                  1:N (well control events)
  │     │
  │     ├── Mud Additive          1:N (materials consumed)
  │     │
  │     └── Job Supply            1:N (supplies consumed)
  │           └── Supply Amount   1:N (quantities by type)
  │
  └── Stimulation                 1:N (frac/acid jobs — straddles both models)
        ├── Stim Stage            1:N (frac stages)
        │     ├── Fluid System    1:N (fluids pumped)
        │     │     └── Additive  1:N (chemical additives)
        │     └── Proppant        1:N (sand/ceramic pumped)
        └── Swab                  1:N (post-stim cleanup)
```

### Entity Details

#### Job

The umbrella for an operational program on a well. A well may have multiple concurrent jobs: initial drilling, completions, workovers, recompletions, facilities, well servicing, plug & abandonment.

**Validated with HOOEY N731H:** 6 jobs across 4 categories spanning 10 months — Drilling (1 job, $5.0M), Completion (2 jobs: Original + Flowback, $5.4M combined), Facilities (2 jobs: Initial Build + Add-on, $1.3M), General Well Servicing (1 Swab job, $15K). Key insight: multiple jobs run concurrently (Drilling + Facilities overlapped May–June 2025), and AFE vs field estimate variance is trackable per job (Facilities overran: $776K AFE → $1.2M actual).

**Job lifecycle categories observed:**
- **Drilling** — initial well construction (spud to rig release)
- **Completion** — downhole completion + flowback as separate jobs under same AFE
- **Facilities** — surface infrastructure, runs parallel to drilling/completion
- **Well Servicing** — post-completion intervention (swab, workover, maintenance)

| Attribute | Description |
|-----------|-------------|
| **Natural key** | `IDRec` on `wvJob` |
| **Parent** | Well (via `IDWELL`) |
| **Core attributes** | Job category (drilling/completion/facilities/well servicing), type, status, start/end dates, target depth/formation |
| **Cost attributes** | AFE amount, field estimate total, final invoice, forecast, variances (all calc columns) |
| **Source tables** | `wvJob` + 40 job-level calc tables |
| **Staging models** | `stg_wellview__jobs` |
| **Existing marts** | `dim_job` |

#### Daily Report

The daily operational record for a job. This is the **heartbeat** of drilling operations — one record per day per job, with child records for costs, time, safety, personnel, materials.

| Attribute | Description |
|-----------|-------------|
| **Natural key** | `IDRec` on `wvJobReport` |
| **Parent** | Job (via `IDRecParent`) |
| **Core attributes** | Report date, report number, depth start/end, activity summary |
| **Key measures** | Depth drilled, footage, costs, time breakdown, safety stats |
| **Source tables** | `wvJobReport` + ~55 daily report calc tables |
| **Staging models** | `stg_wellview__job_reports` |

The daily report is the grain for most operational KPIs: daily burn rate, depth progress, time efficiency, NPT tracking, safety compliance.

#### Phase

A planned stage within a job's program (e.g., "Drill Surface," "Run 9-5/8 Casing," "Cement Surface"). Phases enable planned-vs-actual analysis.

**Validated with HOOEY N731H:** 14 phases on the Drilling job — 9 with actuals (Mob/Rig Up 0.06 days → Surface Drill 1.83 days / 3,506 ft → Surface Casing & Cement → Intermediate Drill 7.13 days / 5,874 ft → Intermediate Casing & Cement → Production Drill Curve 2.75 days / 720 ft → Production Drill 7.21 days / 9,079 ft → Production Casing & Cement) plus 5 placeholder phases (Toe-Prep, Drillout, Stimulation, 2× Flowback) with no actuals. Demonstrates planned-vs-actual tracking and the drill-case-drill-case-drill-case progression.

| Attribute | Description |
|-----------|-------------|
| **Natural key** | `IDRec` on `wvJobProgramPhase` |
| **Parent** | Job (via `IDRecParent`) |
| **Core attributes** | Phase name, type, planned duration (ML/min/max), actual duration |
| **Calc tables** | 17 phase-level rollups + 11 phase-type rollups |
| **Staging models** | `stg_wellview__job_program_phases` |
| **Existing marts** | `dim_phase` |

#### AFE (Job-Level)

Authorization for Expenditure tied to a specific job. Tracks budgeted costs vs actuals.

| Attribute | Description |
|-----------|-------------|
| **Natural key** | `IDRec` on `wvJobAFE` |
| **Parent** | Job (via `IDRecParent`) |
| **Core attributes** | AFE number, supplemental number, project ref, type |
| **Source tables** | `wvJobAFE`, `wvJobAFECost` |
| **Staging models** | `stg_wellview__job_afe_definitions` |
| **Existing marts** | `bridge_job_afe` |

**Cross-system note:** WellView AFEs are *operational authorizations* — "we're approved to spend $X on this job." ODA AFEs (`stg_oda__afe_v2`) are *accounting entities* — they track actual GL postings against AFE numbers. The WellView AFE tells you the budget; ODA tells you what was actually booked.

#### Stimulation (dual membership — Physical Well + Well Work)

Stimulations live at the intersection of Physical Well and Well Work. The *activity* (pumping fluids and proppant) is well work. The *result* (perforated intervals, proppant in the rock, zone production changes) modifies the physical well.

**Validated with HOOEY N731H:** 1 Sand Frac by Liberty Energy (Jul 18–29 2025). Physical attributes: 64 stages, 512 clusters (8/stage), 14M lbs proppant — tightly coupled with 512 perforations. Operational attributes: $4.7M total cost, tracked as a distinct activity within the Completion job window. This confirms dual membership: the stimulation is both a physical completion attribute (stage/cluster/proppant geometry) and an operational event (contractor, schedule, cost).

| Attribute | Description |
|-----------|-------------|
| **Natural key** | `IDRec` on `wvStim` |
| **Parent** | Well (via `IDWELL`) |
| **Core attributes** | Type (frac/acid), date, target formation, stage count, cluster count |
| **Physical Well facet** | Stage geometry, cluster spacing, proppant placement, perforation coupling |
| **Well Work facet** | Contractor, schedule (start/end dates), cost, fluid volumes pumped |
| **Child entities** | Stages → Fluid Systems → Additives; Stages → Proppants |
| **Calc tables** | `wvStimIntPerfCalc` (perf linkage), `wvStimPropTypSumCalc` (proppant summary) |
| **Staging models** | Staged (8 models across stim hierarchy) |

#### Cost, Time Log, NPT, Safety, Personnel

These are the **fact-producing entities** that hang off Daily Report (and roll up to Job/Phase via calc tables). They share a common pattern: detailed line items at daily grain, with system-computed rollups at phase and job grain.

| Entity | Grain | Key Measures | Calc Table Coverage | Staging |
|--------|-------|-------------|-------------------|---------|
| **Cost** | Daily report line item | AFE amount, field estimate, final invoice, forecast, variance | 34 tables across 3 grains | `stg_wellview__daily_costs`, `stg_wellview__daily_recurring_costs` |
| **Time Log** | Activity entry | Duration, % total, by 4-level code hierarchy | 16 tables across 3 grains | `stg_wellview__job_time_log` |
| **NPT** | Incident | Duration, type, category | 5 tables across 2 grains | `stg_wellview__job_interval_problems` |
| **Safety** | Incident or check | Frequency, rate, days since last | 6 tables across 1 grain | `stg_wellview__safety_checks` (57,856 rows), `stg_wellview__safety_incidents` (4,711 rows) |
| **Personnel** | Company/role entry | Headcount, regular/OT hours | 5 tables across 2 grains | `stg_wellview__daily_personnel_logs` |

**Safety staging note:** `wvJobSafetyChk` and `wvJobSafetyIncident` are both staged. Safety records use `IDRecParent` linking directly to **Jobs** (not Daily Reports) — 100% match rate. Both tables are in the `FORMENTERAOPS_WV120_CALC` schema despite being primary data (not calc).

---

## Cross-System Resolution

WellView entities connect to other source systems. This section maps the resolution patterns.

### Well Identity Resolution

The Well entity exists in every source system with different identifiers. Resolution is handled by the `int_well__spine` + `well_360` pattern:

| System | Well Identifier | Resolution Path |
|--------|----------------|-----------------|
| **WellView** | `IDWELL` (GUID), EID, API-10, Cost Center | Primary via EID (6-char) |
| **ProdView** | `IDFLOWNET` (GUID), Property EID, API-10 | Match via EID; linked via System Integration table |
| **ODA** | Cost Center Code (right 6 = EID), API Number | Match via EID derived from code |
| **Combo Curve** | PHDWin ID (= EID), API-10 | Direct EID match |
| **Enverus** | API-10/14 | Match via API-10 from spine (gap-fill only) |

**Existing implementation:** `int_well__spine` unions EIDs from all internal systems. `well_360` joins source-specific prep models (`int_well__wellview`, `int_well__prodview`, `int_well__oda`, `int_well__combo_curve`) onto the spine with COALESCE priority per attribute domain.

### WellView ↔ ProdView

| WellView Entity | ProdView Entity | Link Mechanism |
|-----------------|-----------------|----------------|
| Well (`wvWellHeader`) | Unit (`pvUnit`) | System Integration table (`stg_wellview__system_integrations`) |
| Zone (`wvZone`) | Completion (`pvUnitComp`) | No direct link — aligned by depth/formation |
| Zone Production (`wvZoneProdTypDataCalc`) | Daily Production (`pvDailyProduction`) | Validation only — different grains |
| Completion (`wvCompletion`) | Completion (`pvUnitComp`) | Conceptual overlap, no FK — see note above |

### WellView ↔ ODA

| WellView Entity | ODA Entity | Link Mechanism |
|-----------------|------------|----------------|
| Well | Cost Center (`stg_oda__wells`) | EID (WV) = right(code, 6) (ODA) |
| Job AFE | AFE (`stg_oda__afe_v2`) | AFE Number string match |
| Job Cost | GL Entries (`stg_oda__gl`) | AFE number, cost center |
| Agreement Interest | Revenue/Expense Deck Participants | Conceptual overlap — see Land & Mineral section |

---

## Enterprise Entity Model — Stub

WellView's Physical Well and Well Work models are two chapters in a larger enterprise entity model. The sections below stub out the remaining chapters with their primary source systems and key relationships back to WellView entities.

### Production (Primary: ProdView)

**Question:** "What did the well produce, and how was it allocated?"

```
Unit (≈ Well)                     ← linked to WellView Well via System Integration
  ├── Completion                  ← producing interval (overlaps WellView Zone/Completion)
  │     └── Daily Production      ← oil/gas/water/NGL volumes at completion grain
  ├── Allocation                  ← how measured volumes split across completions
  ├── Downtime                    ← production interruptions
  ├── Production Parameters       ← tubing pressure, casing pressure, choke size
  └── Tank / Meter                ← measurement infrastructure
```

**Key entities:** Unit, Completion, Daily Production, Allocation, Downtime
**Source context:** `context/sources/prodview/prodview.md`
**Existing models:** `int_prodview__production_volumes`, `int_prodview__well_header`, `fct_eng_well_header`
**Connection to WellView:** WellView provides the well's physical context (wellbore geometry, zones, equipment). ProdView provides what it actually produced.

### Finance (Primary: ODA / Quorum)

**Question:** "What did it cost, what did it earn, and where did the money go?"

```
Cost Center (≈ Well)              ← linked to WellView Well via EID
  ├── General Ledger              ← every financial transaction
  │     └── GL Line Item          ← account, amount, period, company
  ├── AFE                         ← capital authorization (complements WellView job-level AFE)
  ├── Revenue Deck                ← who gets paid from production revenue
  │     ├── Deck Revision         ← effective-dated ownership snapshots
  │     └── Deck Participant      ← owner + interest type + decimal
  ├── Expense Deck                ← who pays for operating costs
  │     ├── Deck Revision
  │     └── Deck Participant
  ├── JIB (Joint Interest Billing) ← cost sharing between working interest partners
  │     └── JIB Detail
  ├── AR Invoice                  ← revenue distributions to owners
  │     ├── Invoice Payment
  │     ├── Invoice Adjustment
  │     └── Invoice Netting
  └── AP Invoice                  ← vendor/operator payments
```

**Key entities:** Cost Center, GL Entry, AFE, Revenue Deck, Expense Deck, JIB, AR/AP Invoice
**Existing models:** `int_gl_enhanced`, `int_accounts_classified`, `int_oda_afe_v2`, `general_ledger`, `los_v5_wells`
**Connection to WellView:** WellView Job AFE ↔ ODA AFE (budget vs actual). WellView Job Cost ↔ ODA GL (operational estimate vs booked amount).

### Land, Mineral & Ownership (Primary: ODA + WellView + TBD)

**Question:** "Who owns the minerals, who has the right to produce, and who gets paid what?"

This is the **chain of title** from mineral rights through to royalty payments. It spans multiple systems and may eventually warrant a dedicated land management system.

```
Mineral Interest                  ← who owns the minerals under the surface
  │
  ├── Lease                       ← the legal right to explore/produce
  │     ├── Lease Terms           ← royalty rate, primary term, HBP clauses
  │     └── Lease Parties         ← lessor (mineral owner) + lessee (operator)
  │
  ├── Division Order              ← the legal document defining payment shares
  │     └── Division Interest     ← each owner's decimal interest
  │
  ├── Working Interest            ← operator's share of costs AND revenue
  │     └── WI Partners           ← JIB participants who share costs
  │
  ├── Net Revenue Interest        ← operator's share of revenue after royalties/overrides
  │
  ├── Overriding Royalty Interest  ← carved-out revenue interests (ORRIs)
  │
  └── Revenue/Expense Allocation  ← how production value flows to owners
        ├── Revenue Deck          ← ODA: who gets revenue, by effective date
        └── Expense Deck          ← ODA: who pays costs, by effective date
```

**Where this data lives today:**

| Data | System | Tables/Models |
|------|--------|---------------|
| WI/NRI snapshot (well-level) | WellView | `wvWellHeader.UserNum1-5` (WI, NRI total, NRI-WI, override, mineral royalty) |
| Agreement details | WellView | `wvAgreement`, `wvAgreementInt` (partners + interest types), `wvAgreementDate`, `wvAgreementLink` |
| Revenue ownership by period | ODA | `stg_oda__revenue_deck_v2`, `_revision`, `_participant`, `_set` |
| Expense ownership by period | ODA | `stg_oda__expense_deck_v2`, `_revision`, `_participant`, `_set` |
| JIB cost sharing | ODA | `stg_oda__jib`, `stg_oda__jibdetail` |
| Owner master | ODA | `stg_oda__owner_v2` |
| Interest types | ODA | `stg_oda__interest_type` |
| WI/NRI point-in-time | ODA | `int_oda_latest_company_WI`, `int_oda_latest_company_NRI` |

**Gaps and open questions:**
- No dedicated land management system (Quorum Land, P2 Land, etc.) — mineral interest and lease data may be tracked in spreadsheets or WellView's Agreement module
- `wvAgreement` / `wvAgreementInt` are unstaged — these contain partner interest details that complement ODA revenue/expense decks
- **Confirmed gap (HOOEY N731H):** 0 agreements found on a recently drilled/completed well with $11.8M in costs and presence in all 4 source systems. WI/NRI also null on well header `UserNum1-5`. This suggests agreement and ownership data is either not being entered in WellView or lives entirely in ODA decks for this well.
- Division order data may live outside both WellView and ODA
- The full ownership chain (mineral → lease → division order → deck participant → payment) has not been modeled end-to-end
- WellView stores a *snapshot* of WI/NRI on the well header; ODA stores the *effective-dated history* via deck revisions — these must be reconciled

**Future mart candidates:**
- `dim_owner` — master owner entity across ODA + WellView agreements
- `fct_ownership_history` — effective-dated WI/NRI/ORRI by well, from deck revisions
- `bridge_well_owner` — M:N relationship between wells and interest holders
- `fct_revenue_distribution` — how production revenue flows from wellhead to owner bank accounts

### Economics (Primary: Combo Curve)

**Question:** "What is the well worth, and what will it produce in the future?"

```
Economic Run                      ← a valuation scenario
  ├── Economic Well               ← well-level inputs and outputs
  │     ├── EUR / Type Curve      ← production forecast
  │     ├── NPV / Cash Flow       ← discounted economics
  │     └── Pricing Assumptions   ← commodity price inputs
  └── Economic Project            ← multi-well roll-up (development program)
```

**Key entities:** Economic Run, Economic Well, Type Curve, NPV
**Source context:** `context/sources/combo_curve/` (not yet documented)
**Existing models:** `int_economic_runs_with_one_liners`, `economics` mart
**Connection to WellView:** Wells matched via EID/PHDWin ID. Combo Curve EUR vs WellView actual production (via ProdView) enables forecast accuracy analysis.

### Market Pricing (Primary: Aegis)

**Question:** "What are commodities worth today?"

```
Price Index                       ← benchmark pricing point
  └── Daily/Monthly Price         ← price by date and product
```

**Key entities:** Price Index, Price History
**Connection to WellView:** Indirect — market prices apply to production volumes (ProdView) and economic assumptions (Combo Curve), not directly to WellView.

### CRM (Primary: HubSpot)

**Question:** "Who are our business contacts?"

Minimal scope — 1 table, currently only used for contact management. May eventually link to ODA owner records or WellView agreement contacts.

---

## Mart Roadmap

Based on the entity models above, these are the marts that should eventually exist. Grouped by domain with dependencies noted.

### Physical Well Marts

| Mart | Type | Key Entities | Status | Dependencies |
|------|------|-------------|--------|--------------|
| `well_360` | Dimension | Well (golden record) | **Exists** | `int_well__spine` + source prep models |
| `dim_wellbore` | Dimension | Wellbore, Sections, Key Depths | **Exists** | `stg_wellview__wellbores` |
| `dim_well_survey` | Dimension | Directional Survey, Stations | **Exists** | `stg_wellview__wellbore_directional_surveys`, `stg_wellview__wellbore_directional_survey_data` |
| `dim_zone` | Dimension | Zone, Status, Formation | **Exists** | `stg_wellview__zones`. **Note:** primarily useful for vertical/legacy wells (~472 zones, ~161 wells). Horizontal wells use zones as a simple target formation tag (~1 zone/well, sparse metadata). |
| `dim_completion` | Dimension | Completion, Zones, Links | **Low priority** | Only 23 records total in WellView. Horizontal well completions better sourced from Stimulation + Perforation entities or ProdView. Staging not warranted until vertical well management becomes a priority. |
| `dim_well_equipment` | Dimension | Casing + Tubing + Rods + Perfs + Prod Settings | **Exists** | Built from staged tubing/rod/perforation/prod-settings with COALESCE lift-type inference: (1) `PRODMETHTYP`, (2) equipment inference, (3) `setting_objective`. Current model is a well-level snapshot. |
| `fct_well_configuration` | Fact | Point-in-time well configuration snapshot | Not started | Multiple Physical Well entities |

### Well Work Marts

| Mart | Type | Key Entities | Status | Notes |
|------|------|-------------|--------|-------|
| `dim_job` | Dimension | Job | **Exists** | `stg_wellview__jobs` |
| `dim_phase` | Dimension | Phase | **Exists** | `stg_wellview__job_program_phases` |
| `bridge_job_afe` | Bridge | Job ↔ AFE | **Exists** | `stg_wellview__job_afe_definitions` |
| `fct_daily_drilling` | Fact | Daily Report + Cost + Time | **Exists** | Combined daily drilling summary |
| `fct_daily_drilling_cost` | Fact | Cost at daily grain | **Exists** | Sprint 1, incremental, 1.9M rows; source calc tables `wvJRCostCalc` |
| `fct_drilling_time` | Fact | Time breakdown by activity code | **Exists** | Sprint 2, table, 762K rows; source calc tables `wvJTLSumCalc`, `wvJRTLSumCalc` |
| `fct_npt_events` | Fact | Non-productive time events | **Exists** | Sprint 2, table, 14K rows; source `stg_wellview__job_interval_problems` |
| `fct_safety_events` | Fact | Incidents + checks (UNION) | **Exists** | Sprint 3, table, 62,567 rows; `event_type` discriminator (`check`/`incident`); parent links to Jobs not Daily Reports |
| `fct_stimulation` | Fact | Frac operations + proppant | **Exists** | Sprint 3, table, 3,838 rows; stim-job grain with WellView calc rollups |
| `fct_drilling_performance` | Fact | ROP, stand KPIs, slide/rotate | Not started | Calc tables: `wvJRigActivityCalc`, `wvJDSSlideSheetCalc` |

### Cross-Domain Marts (require multiple entity models)

| Mart | Type | Models Required | Status |
|------|------|----------------|--------|
| `fct_well_cost_vs_budget` | Fact | Well Work (WV Job AFE) + Finance (ODA AFE + GL) | Not started |
| `fct_ownership_history` | Fact | Land/Mineral (ODA decks) + Physical Well (WV well) | Not started |
| `dim_owner` | Dimension | Owner master across systems | **Exists** — `models/operations/marts/orm/dim_owner.sql` (64,387 rows). **Currently HubSpot-sourced** (PR #266): UNION of contact-based (19K) + company-only (45K) HubSpot owners. WellView `wvAgreement`/`wvAgreementInt` data and ODA `stg_oda__owner_v2` are NOT yet incorporated. Confirmed gap: HOOEY N731H shows 0 WellView agreements on a recently completed well — ownership likely lives in ODA decks. |
| `fct_well_economics_vs_actual` | Fact | Economics (CC EUR) + Production (PV volumes) | Not started |

---

## Appendix: Calc Table Mapping to Entity Model

The 144 calc tables map to entities in this model as follows:

| Entity Model | Entity | Calc Table Prefix | Count | See Also |
|---|---|---|---|---|
| Physical Well | Wellbore | `wvWB*Calc`, `wvWBF*Calc` | 8 | `calc_table_blueprint.md` — Well Infrastructure |
| Physical Well | Survey | `wvWComp*Calc`, `wvWDS*Calc` | 4 | |
| Physical Well | Zone | `wvZone*Calc` | 3 | |
| Physical Well | Stimulation | `wvStim*Calc` | 2 | |
| Physical Well | AFE (well-level) | `wvAFE*Calc` | 2 | |
| Well Work | Job | `wvJ*Calc` (non-report) | ~40 | `calc_table_blueprint.md` — Operations |
| Well Work | Daily Report | `wvJR*Calc` | ~55 | |
| Well Work | Phase | `wvJPP*Calc`, `wvJPPCode1*Calc` | ~28 | |
| Well Work | Drill String | `wvJDS*Calc` | 6 | |
| Well Work | Time Log | `wvJTL*Calc` | 3 | |
| Well Work | Stand | `wvJStand*Calc` | 2 | |
