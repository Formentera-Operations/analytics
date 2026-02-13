# WellView Calc Table Analytics Blueprint

Reference for building entity models, metrics, and semantic layer definitions from WellView's 144 precalculated reporting tables.

## How to Use This Document

- Load when planning intermediate/mart models or semantic layer work
- Cross-reference with domain YAMLs in `context/sources/wellview/` for column-level detail
- Calc tables are precalculated by WellView — use as validation targets and metric templates, not source-of-truth
- Table names map to Snowflake as `WVT_WV<NAME>` in `FORMENTERAOPS_WV120_CALC` schema

---

## Entity Hierarchy (from Calc Tables)

The 144 calc tables organize around two main entity trees: **Operations** (drilling/completion jobs) and **Well Infrastructure** (wellbore, zones, stimulations, AFEs).

### Operations Entity Tree (127 calc tables)

```
Job (wvJ*Calc — 40 job-level rollups)
  │
  ├── Daily Report (wvJR*Calc — 55 daily report rollups)
  │   ├── Cost (14 tables)
  │   │   ├── wvJRCostCalc — daily cost line items
  │   │   ├── wvJRCostCumCalc — cumulative cost to date
  │   │   ├── wvJRCostBaseCalc — base cost cum
  │   │   ├── wvJRAFECostCalc — daily cost by AFE
  │   │   ├── wvJRCostCumAFECalc — cumulative cost by AFE
  │   │   ├── wvJRCostDesCumCalc — cum by description
  │   │   ├── wvJRCostCode1CumCalc — cum by code level 1
  │   │   ├── wvJRCostCode2CumCalc — cum by code level 2
  │   │   ├── wvJRCostCode3CumCalc — cum by code level 3
  │   │   ├── wvJRCostCode4CumCalc — cum by code level 4
  │   │   ├── wvJRCostCode5CumCalc — cum by code level 5
  │   │   ├── wvJRCostCode6CumCalc — cum by code level 6
  │   │   ├── wvJRCostMudCalc — mud additive costs
  │   │   └── wvJRCostJobSupplyCalc — job supply costs
  │   │
  │   ├── Time Log (9 tables)
  │   │   ├── wvJobReportTimeLogCalc — time log entries for report
  │   │   ├── wvJRTLSumCalc — daily time log summary
  │   │   ├── wvJRTLCumCalc — cum time log to date
  │   │   ├── wvJRTLCumCode1Calc — cum by code 1
  │   │   ├── wvJRTLCumCode2Calc — cum by code 2
  │   │   ├── wvJRTLCumCode3Calc — cum by code 3
  │   │   ├── wvJRTLCumCode4Calc — cum by code 4
  │   │   ├── wvJRTLCumOpsCatCalc — cum by ops category
  │   │   └── wvJRTLCumUnschedTypCalc — cum by unscheduled type
  │   │
  │   ├── NPT / Interval Problems (3 tables)
  │   │   ├── wvJRIntervalProblemCalc — NPT events during report
  │   │   ├── wvJRIntervalProblemSumCalc — NPT by type
  │   │   └── wvJRIntProbCatCalc — NPT by major category
  │   │
  │   ├── Safety (4 tables)
  │   │   ├── wvJRSafetyChkCalc — safety check summary
  │   │   ├── wvJRSafetyChkDesCalc — safety check by description
  │   │   ├── wvJRSafetyIncCalc — safety incident summary
  │   │   └── wvJRSafetyIncTyp1Calc — incidents by type
  │   │
  │   ├── Personnel (3 tables)
  │   │   ├── wvJRPersCtSumCalc — headcount by company type
  │   │   ├── wvJRPersCtRefDerrickCalc — headcount by derrick ref
  │   │   └── wvJRPersCtCompanyCalc — headcount by company
  │   │
  │   ├── Materials (4 tables)
  │   │   ├── wvJRJobSupplyCalc — job supply usage
  │   │   ├── wvJRMudAddCalc — mud additive usage
  │   │   ├── wvJRMudVolCalc — mud volume summary
  │   │   └── wvJR30hrTimeLogCalc — extended time breakdown
  │   │
  │   ├── Fluids (3 tables)
  │   │   ├── wvJRFluidsCalc — fluids by type
  │   │   ├── wvJRFluidsActionCalc — fluids by type + subtype
  │   │   └── wvJRFluidsZoneCompCalc — fluids by zone/completion
  │   │
  │   ├── Rig Activity & Stand KPIs (7 tables)
  │   │   ├── wvJRRigActivityCalc — rig activity by Type 1
  │   │   ├── wvJRRigActivityDtlCalc — rig activity by Type 2
  │   │   ├── wvJRRCCalc — rig crew list
  │   │   ├── wvJRRCRigActivityCalc — rig activity by crew + Type 1
  │   │   ├── wvJRRCRigActivityDtlCalc — rig activity by crew + Type 2
  │   │   ├── wvJRRCStandCalc — stand KPIs by crew
  │   │   └── wvJRStandCalc — stand KPIs for report
  │   │
  │   ├── Other Daily (4 tables)
  │   │   ├── wvJRProgramPhaseCalc — phases during report
  │   │   ├── wvJRTestEquipCalc — equipment pressure tests
  │   │   ├── wvJRCasSummaryCalc — casing to surface summary
  │   │   ├── wvJRDetailComCalc — derived sensor time codes
  │   │   └── wvJROfflineTimeLogCalc — offline time log events
  │   │
  │   └── (total: ~55 daily report calc tables)
  │
  ├── Program Phase (wvJPP*Calc — 17 phase-level rollups)
  │   ├── wvJPPActivitySumCalc — planned vs actual durations
  │   ├── wvJPPCostCalc — phase costs
  │   ├── wvJPPTLCalc — phase time log breakdown
  │   ├── wvJPPTLOpsCatCalc — phase time by ops category
  │   ├── wvJPPTLUnschedTypCalc — phase time by unscheduled type
  │   ├── wvJPPFluidsCalc — phase fluids by type
  │   ├── wvJPPFluidsActionCalc — phase fluids by type + subtype
  │   ├── wvJPPMudAdCalc — phase mud additive usage
  │   ├── wvJPPJobSupCalc — phase job supply usage
  │   ├── wvJPPIntervalProblemCalc — NPT during phase
  │   ├── wvJPPVendorCalc — vendors with costs for phase
  │   ├── wvJPPDrillStringCalc — drill strings during phase
  │   ├── wvJPPRigActivityCalc — rig activity by Type 1
  │   ├── wvJPPRigActivityDtlCalc — rig activity by Type 2
  │   └── wvJPPStandCalc — stand KPIs for phase
  │
  │   └── Phase Type 1 (wvJPPCode1*Calc — 11 rollups by phase type)
  │       ├── wvJPPCode1Calc — phase summary by type 1
  │       ├── wvJPPCode1CostCalc — cost by phase type
  │       ├── wvJPPCode1TLCalc — time log by phase type
  │       ├── wvJPPCode1TLOpsCatCalc — time by ops cat by phase type
  │       ├── wvJPPCode1TLUnschedTypCalc — time by unsched type
  │       ├── wvJPPCode1MudAdCalc — mud additive by phase type
  │       ├── wvJPPCode1JobSupCalc — job supply by phase type
  │       ├── wvJPPCode1FluidsCalc — fluids by phase type
  │       ├── wvJPPCode1FluidsActionCalc — fluid actions by phase type
  │       └── wvJPPCode1IntervalProbCalc — NPT by phase type
  │
  ├── Drill String (wvJDS*Calc — 6 tables)
  │   ├── wvJDSDPHydCalc — hydraulic calculations
  │   ├── wvJDSDPAVCalc — annular velocity / BHP calcs
  │   ├── wvJDSRigActivityCalc — rig activity by Type 1
  │   ├── wvJDSRigActivityDtlCalc — rig activity by Type 2
  │   ├── wvJDSStandCalc — stand KPIs per drill string
  │   └── wvJDSSlideSheetCalc — rotate vs slide from sensor data
  │
  ├── Time Log Entry (wvJTL*Calc — 3 tables)
  │   ├── wvJTLRigActivityCalc — rig activity by time log entry
  │   ├── wvJTLRigActivityDtlCalc — rig activity detail
  │   └── wvJTLStandCalc — stand KPIs per time log entry
  │
  ├── Stand (wvJStand*Calc — 2 tables)
  │   ├── wvJStandRigActivityCalc — rig activity by stand
  │   └── wvJStandRigActivityDtlCalc — rig activity detail by stand
  │
  └── Job-Level Rollups (wvJ*Calc — remaining ~40 tables)
      │
      ├── Cost Summary (20 tables)
      │   ├── wvJCostCumCalc — cumulative job cost (AFE + field + invoice + forecast + variances)
      │   ├── wvJAFECostCumCalc — cumulative cost by AFE
      │   ├── wvJCostSumAFEDesCalc — AFE cost by account name
      │   ├── wvJCostSumAFECode1Calc — AFE cost by main account
      │   ├── wvJCostSumAFECode2Calc — AFE cost by sub account
      │   ├── wvJCostSumAFECode3Calc — AFE cost by AFE category
      │   ├── wvJCostSumAFECode4Calc — AFE cost by expense type
      │   ├── wvJCostSumAFECode5Calc — AFE cost by tangible/intangible
      │   ├── wvJCostSumAFECode6Calc — AFE cost by code 6
      │   ├── wvJCostSumDailyDesCalc — daily cost by description
      │   ├── wvJCostSumDailyCode1Calc — daily cost by code 1
      │   ├── wvJCostSumDailyCode2Calc — daily cost by code 2
      │   ├── wvJCostSumDailyCode3Calc — daily cost by code 3
      │   ├── wvJCostSumDailyCode4Calc — daily cost by code 4
      │   ├── wvJCostSumDailyCode5Calc — daily cost by code 5
      │   ├── wvJCostSumDailyCode6Calc — daily cost by code 6
      │   ├── wvJPONoCalc — field estimates by PO + vendor
      │   ├── wvJVendorCalc — vendor cost totals
      │   ├── wvJVendorTicketNoCalc — vendor cost by ticket
      │   └── wvJSumOpsCatCalc — cost + time by ops category
      │
      ├── Time Log Summary (7 tables)
      │   ├── wvJTLSumCalc — time log totals (duration + % total)
      │   ├── wvJTLSumCode1Calc — by code 1
      │   ├── wvJTLSumCode2Calc — by code 2
      │   ├── wvJTLSumCode3Calc — by code 3
      │   ├── wvJTLSumCode4Calc — by code 4
      │   ├── wvJTLSumOpsCatCalc — by ops category
      │   └── wvJTLSumUnschedTypCalc — by unscheduled type
      │
      ├── Safety & NPT (2 tables)
      │   ├── wvJIntProbCatCalc — NPT by major category
      │   └── wvJPersonnelCountCalc — personnel by company + type
      │
      ├── Personnel (2 tables)
      │   ├── wvJPersCtSumCalc — headcount by type
      │   └── wvJPersCtCompanyCalc — headcount by company
      │
      ├── Fluids & Materials (3 tables)
      │   ├── wvJFluidsCalc — fluids by type for job
      │   ├── wvJFluidsActionCalc — fluids by type + subtype
      │   └── wvJDischargeCumCalc — discharge summary
      │
      ├── Rig Activity & Sensor (4 tables)
      │   ├── wvJRigActivityCalc — rig activity by Type 1 (job level)
      │   ├── wvJRigActivityDtlCalc — rig activity by Type 2
      │   ├── wvJStandRigActivityCalc — rig activity by stand
      │   └── wvJStandRigActivityDtlCalc — stand rig activity detail
      │
      └── Performance (1 table)
          └── wvJPerformanceCalc — BHA runs, bit runs, depth, ROP by year/month

```

### Well Infrastructure Entity Tree (17 calc tables)

```
Wellbore (wvWB*Calc — 4 tables)
  ├── wvWellboreSummaryCalc — actual wellbore section summary (size, depth, dates, mud density)
  ├── wvWellboreSumExcludeCalc — wellbore summary excluding certain sections
  ├── wvWBRigActivityCalc — rig activity by wellbore
  └── wvWBRigActivityDtlCalc — rig activity detail by wellbore
      ├── wvWBStandCalc — stand KPIs per wellbore

Wellbore Formation (wvWBF*Calc — 3 tables)
  ├── wvWBFRigActivityCalc — rig activity by formation
  ├── wvWBFRigActivityDtlCalc — rig activity detail by formation
  └── wvWBFStandCalc — stand KPIs per formation

Directional Survey (wvWComp*/wvWDS*Calc — 4 tables)
  ├── wvWCompDirSurveyCalc — composite surveys (actual)
  ├── wvWCompDirSurveyPropCalc — composite surveys (proposed)
  ├── wvWDSVSCalc — survey curves with adjusted vertical section
  └── wvWDSVSDataCalc — survey data points with adjusted VS

Zone (wvZone*Calc — 3 tables)
  ├── wvZoneFormationCalc — formation(s) intersecting zone depths
  ├── wvZoneProdTypCalc — distinct production activity types for zone
  └── wvZoneProdTypDataCalc — production volumes by activity type

Stimulation (wvStim*Calc — 2 tables)
  ├── wvStimIntPerfCalc — perforations linked by frac stage depths
  └── wvStimPropTypSumCalc — proppant mass by type/subtype/size

AFE (wvAFE*Calc — 2 tables, in other.yaml)
  ├── wvAFECalc — well-level AFE cost summary (across jobs)
  └── wvAFECostSumCalc — cumulative cost by AFE (across jobs)
```

---

## Grain Map & Naming Convention

### Prefix Decoding

Every calc table name follows the pattern `wv{Scope}{Subject}Calc` where the prefix identifies the grain:

| Prefix | Grain | Parent Entity | Count | Example |
|---|---|---|---|---|
| `wvJR*Calc` | Daily Report | `wvJobReport` | 55 | `wvJRCostCalc` — costs for one daily report |
| `wvJPP*Calc` | Program Phase | `wvJobProgramPhase` | 17 | `wvJPPCostCalc` — costs for one phase |
| `wvJPPCode1*Calc` | Phase Type 1 | `wvJobProgramPhase.PhaseType1` | 11 | `wvJPPCode1CostCalc` — costs by phase type |
| `wvJDS*Calc` | Drill String | `wvJobDrillString` | 6 | `wvJDSRigActivityCalc` — activity per drill string |
| `wvJTL*Calc` | Time Log Entry | `wvJobTimeLog` | 3 | `wvJTLRigActivityCalc` — rig activity per TL entry |
| `wvJStand*Calc` | Stand | `wvJStand` | 2 | `wvJStandRigActivityCalc` — rig activity per stand |
| `wvJ*Calc` | Job | `wvJob` | 40 | `wvJCostCumCalc` — cumulative cost for job |
| `wvWB*Calc` | Wellbore | `wvWellbore` | 4 | `wvWBRigActivityCalc` — rig activity per wellbore |
| `wvWBF*Calc` | Wellbore Formation | `wvWellboreFormation` | 3 | `wvWBFStandCalc` — stand KPIs per formation |
| `wvWComp*Calc` | Wellbore (composite) | `wvWellbore` | 2 | `wvWCompDirSurveyCalc` — composite survey |
| `wvWDS*Calc` | Dir Survey (cross-WB) | `wvWellboreDirSurvey` | 2 | `wvWDSVSCalc` — survey VS recalculated |
| `wvZone*Calc` | Zone | `wvZone` | 3 | `wvZoneProdTypDataCalc` — production by zone |
| `wvStim*Calc` | Stimulation | `wvStim` | 2 | `wvStimPropTypSumCalc` — proppant by type |
| `wvAFE*Calc` | AFE (well-level) | `wvWellHeader` | 2 | `wvAFECalc` — AFE summary across jobs |

### Subject Patterns (What the Calc Measures)

| Subject Token | Meaning | Common Measures |
|---|---|---|
| `Cost` / `CostCum` | Cost rollup | AFE amount, field estimate, final invoice, forecast, variances (normalized + raw) |
| `CostCode{1-6}Cum` | Cost by hierarchy level | Same cost measures, grouped by 6-level cost code hierarchy |
| `TLSum` / `TLCum` | Time log summary | Duration (days), % total time, by code 1-4 / ops category / unscheduled type |
| `RigActivity` | Sensor-derived states | Duration total/on-btm/off-btm/pipe moving, ROP, WOB, RPM, flow rate, SPP, torque |
| `RigActivityDtl` | Rig activity detail (Type 2) | Same measures as RigActivity, at finer classification |
| `Stand` | Stand KPIs | Duration avg/min/max/med/stddev for: stand, connection, on-btm, off-btm |
| `SlideSheet` | Rotate vs slide | Duration, depth progress, ROP, WOB, RPM, flow rate, SPP, torque, inclination |
| `IntervalProblem` | NPT events | Problem duration, cum duration, exclude flag |
| `IntProbCat` | NPT by category | Category, duration, cum |
| `SafetyChk` | Safety checks | Type, frequency, last date, days since last, next date, days until next |
| `SafetyInc` | Safety incidents | Type 1/2, frequency, rate, days since last |
| `PersCt` | Personnel headcount | Company/type, head count, regular/OT/total work hours |
| `MudAd` / `MudVol` | Mud materials | Additive usage, volume additions and losses |
| `JobSup` / `Supply` | Job supplies | Supply usage quantities |
| `Fluids` / `FluidsAction` | Lease/well fluids | Volumes by fluid type and subtype |
| `Vendor` / `PONo` | Vendor costs | Vendor, cost total, ticket/PO numbers |
| `Performance` | Drilling performance | BHA runs, bit runs, depth drilled, ROP, drilling/circulating hours by year/month |
| `DPHyd` / `DPAV` | Hydraulics | Annular velocity, bottom hole pressure |
| `ActivitySum` | Plan vs actual | Planned durations (ML/min/max) vs actual time log, variance |
| `WellboreSummary` | Wellbore sections | Section size, depth top/btm, dates, mud density range |

---

## Metric Catalog by Business Domain

### Tier 1 — Drilling Cost KPIs (Highest Business Value)

Cost tracking is the deepest calc table coverage — 34 tables across 3 grains (daily, phase, job).

| Metric | Source Calc Tables | Grain | Key Measures |
|---|---|---|---|
| **AFE vs Actual Variance** | `wvJCostCumCalc`, `wvAFECalc` | Job, Well | `CostAFE - CostFieldEst`, `CostAFE - CostFinalInvoice` |
| **Daily Cost Burn Rate** | `wvJRCostCalc`, `wvJRCostCumCalc` | Daily Report | Field estimate by code/description/vendor |
| **Cost by Code Hierarchy** | `wvJRCostCode{1-6}CumCalc`, `wvJCostSumDailyCode{1-6}Calc` | Daily/Job | 6 levels: main account → tangible/intangible |
| **Cost by AFE Allocation** | `wvJRAFECostCalc`, `wvJRCostCumAFECalc`, `wvJAFECostCumCalc` | Daily/Job | Cost tied to specific AFE numbers |
| **Vendor Cost Analysis** | `wvJVendorCalc`, `wvJVendorTicketNoCalc`, `wvJPONoCalc` | Job | Vendor totals, by ticket, by PO |
| **Phase Cost Summary** | `wvJPPCostCalc`, `wvJPPCode1CostCalc` | Phase | Daily field estimates by phase/phase type |
| **Material Cost** | `wvJRCostMudCalc`, `wvJRCostJobSupplyCalc` | Daily Report | Mud additive and supply costs |
| **Forecast vs Actual** | `wvJCostCumCalc` | Job | `CostForecast`, `CostAFEForecastVar`, `CostForecastFieldVar` |
| **Well-Level AFE Summary** | `wvAFECalc`, `wvAFECostSumCalc` | Well | Cross-job AFE rollup (AFE + supp + normalized amounts) |

### Tier 1 — Drilling Performance KPIs

| Metric | Source Calc Tables | Grain | Key Measures |
|---|---|---|---|
| **Rate of Penetration (ROP)** | `wvJRigActivityCalc`, `wvJDSSlideSheetCalc` | Job/DS | `ROPStartEnd`, `ROPProgress` (m/day) |
| **Time Breakdown** | `wvJTLSumCalc`, `wvJRTLSumCalc` | Job/Daily | Duration by code hierarchy + ops category + % total time |
| **Planned vs Actual Duration** | `wvJPPActivitySumCalc` | Phase | ML/min/max vs actual, variance |
| **Stand Performance** | `wvJRStandCalc`, `wvJDSStandCalc`, `wvJPPStandCalc` | DR/DS/Phase | Avg/min/max/med/stddev for stand, connection, on-btm, off-btm |
| **Rig Activity by State** | `wvJRigActivityCalc` (+ at 6 other grains) | All grains | Duration on-btm/off-btm/pipe-moving, WOB, RPM, torque, flow rate, SPP |
| **Slide vs Rotate** | `wvJDSSlideSheetCalc` | Drill String | Duration, depth, ROP, WOB, RPM, inclination for slide vs rotate |
| **Monthly Performance Trends** | `wvJPerformanceCalc` | Job (by month) | BHA runs, bit runs, depth drilled, drilling/circ hours |
| **Hydraulics** | `wvJDSDPHydCalc`, `wvJDSDPAVCalc` | Drill String | Annular velocity, bottom hole pressure |
| **NPT Analysis** | `wvJRIntervalProblemCalc`, `wvJIntProbCatCalc` | Daily/Job | Duration, cum duration, by type and major category |

### Tier 1 — Safety & HSE

| Metric | Source Calc Tables | Grain | Key Measures |
|---|---|---|---|
| **Safety Incident Rate** | `wvJRSafetyIncCalc`, `wvJRSafetyIncTyp1Calc` | Daily Report | Frequency, occurrence rate, days since last incident |
| **Safety Check Compliance** | `wvJRSafetyChkCalc`, `wvJRSafetyChkDesCalc` | Daily Report | Frequency, days since last check, days until next |
| **Reportable Incidents** | `SafetyIncReportNoCalc` (column on `wvJobReport`) | Daily Report | Reportable count, cum, TRIR, days since last reportable |

*Note: Key safety metrics also live as calc columns on `wvJobReport` directly — not separate calc tables.*

### Tier 2 — Well Construction & Surveys

| Metric | Source Calc Tables | Grain | Key Measures |
|---|---|---|---|
| **Wellbore Section Summary** | `wvWellboreSummaryCalc` | Wellbore | Section size, depth top/btm, dates, mud density range |
| **Rig Activity by Formation** | `wvWBFRigActivityCalc`, `wvWBFRigActivityDtlCalc` | Formation | Duration, ROP, WOB, RPM by formation |
| **Formation Stand KPIs** | `wvWBFStandCalc` | Formation | Stand/connection/on-btm/off-btm duration stats |
| **Wellbore Rig Activity** | `wvWBRigActivityCalc`, `wvWBRigActivityDtlCalc` | Wellbore | Duration, ROP by wellbore |
| **Composite Surveys** | `wvWCompDirSurveyCalc`, `wvWCompDirSurveyPropCalc` | Wellbore | Actual + proposed composite directional surveys |
| **Cross-Wellbore Survey** | `wvWDSVSCalc`, `wvWDSVSDataCalc` | Well (multi-WB) | VS recalculated across wellbores |
| **Casing Summary** | `wvJRCasSummaryCalc` | Daily Report | Casing strings to surface |

### Tier 2 — Production & Zones

| Metric | Source Calc Tables | Grain | Key Measures |
|---|---|---|---|
| **Zone Production by Activity** | `wvZoneProdTypDataCalc` | Zone | Oil/gas/condensate/water volumes, cum volumes, producing/down time, % uptime |
| **Zone Activity Types** | `wvZoneProdTypCalc` | Zone | Distinct activity types per zone |
| **Zone-Formation Mapping** | `wvZoneFormationCalc` | Zone | Which formations a zone intersects |
| **Fluids by Zone/Completion** | `wvJRFluidsZoneCompCalc` | Daily Report | Well fluid summary by zone/completion |

### Tier 2 — Completions & Stimulation

| Metric | Source Calc Tables | Grain | Key Measures |
|---|---|---|---|
| **Proppant by Type** | `wvStimPropTypSumCalc` | Stimulation | Proppant mass by type, subtype, sand size |
| **Perf-Stim Linkage** | `wvStimIntPerfCalc` | Stim Stage | Perforations linked by frac stage depth interval |

### Tier 3 — Materials & Equipment

| Metric | Source Calc Tables | Grain | Key Measures |
|---|---|---|---|
| **Mud Additive Usage** | `wvJRMudAddCalc`, `wvJPPMudAdCalc` | Daily/Phase | Additive quantities consumed |
| **Mud Volume Summary** | `wvJRMudVolCalc` | Daily Report | Volume additions/losses by type |
| **Job Supply Usage** | `wvJRJobSupplyCalc`, `wvJPPJobSupCalc` | Daily/Phase | Supply quantities consumed |
| **Fluid Volumes** | `wvJRFluidsCalc`, `wvJFluidsCalc` | Daily/Job | Lease/well fluid volumes by type |
| **Discharge Summary** | `wvJDischargeCumCalc` | Job | Cumulative discharge |
| **Equipment Tests** | `wvJRTestEquipCalc` | Daily Report | Equipment pressure test results |

### Tier 3 — Personnel

| Metric | Source Calc Tables | Grain | Key Measures |
|---|---|---|---|
| **Daily Headcount** | `wvJRPersCtSumCalc`, `wvJRPersCtCompanyCalc` | Daily Report | Head count, regular/OT/total hours by type/company |
| **Job Personnel Summary** | `wvJPersonnelCountCalc`, `wvJPersCtSumCalc` | Job | Headcount by company and type for entire job |
| **Derrick Reference** | `wvJRPersCtRefDerrickCalc` | Daily Report | Headcount by derrick reference |

---

## Semantic Layer Roadmap

This section maps calc tables to future dbt Semantic Layer definitions. No semantic layer exists today — these are recommendations for when the team builds one.

### Priority 1 — Cost Entity (`semantic_model: wellview_drilling_costs`)

**Source calc tables:** `wvJCostCumCalc` (job grain), `wvJRCostCalc` / `wvJRCostCumCalc` (daily grain), `wvAFECalc` (well grain)

**Entities:**
- `job` (primary) — linked to `dim_well` via `IDWELL`
- `daily_report` — child of job
- `well` — via `wvAFECalc` for cross-job summaries

**Dimensions:**
- `cost_description`, `code_1` through `code_6`, `vendor`, `afe_number`, `ops_category`

**Measures:**
- `afe_amount`, `field_estimate`, `final_invoice`, `forecast_amount`
- `afe_field_variance` (derived), `afe_invoice_variance` (derived), `forecast_variance` (derived)
- All measures have `normalized_*` variants

**Metrics:**
- `total_well_cost` — SUM of `field_estimate` at well grain
- `afe_variance_pct` — `afe_field_variance / afe_amount`
- `daily_burn_rate` — `field_estimate / reporting_days`
- `cost_by_phase_type` — field estimate grouped by phase type code

### Priority 2 — Time & Activity Entity (`semantic_model: wellview_drilling_time`)

**Source calc tables:** `wvJTLSumCalc` (job), `wvJRTLSumCalc` (daily), `wvJRigActivityCalc` (job sensor)

**Entities:**
- `job` (primary)
- `daily_report`, `phase`, `drill_string`, `wellbore`, `formation`

**Dimensions:**
- `code_1` through `code_4`, `ops_category`, `unscheduled_type`, `rig_activity_type_1`, `rig_activity_type_2`

**Measures:**
- `duration_days`, `pct_total_time`
- Rig activity: `duration_on_btm`, `duration_off_btm`, `duration_pipe_moving`, `rop`, `wob`, `rpm`
- Stand KPIs: `stand_duration_avg`, `connection_time_avg`

**Metrics:**
- `productive_time_pct` — time on-bottom / total time
- `npt_pct` — NPT duration / total duration
- `avg_rop` — depth drilled / drilling hours
- `connection_time_avg` — from stand calc tables

### Priority 3 — Safety Entity (`semantic_model: wellview_safety`)

**Source calc tables:** `wvJRSafetyIncCalc`, `wvJRSafetyChkCalc` + `wvJobReport` safety columns

**Metrics:**
- `total_recordable_incident_rate` (TRIR)
- `days_since_last_incident`
- `safety_check_compliance_rate`

### Priority 4 — Well Construction Entity (`semantic_model: wellview_wellbore`)

**Source calc tables:** `wvWellboreSummaryCalc`, `wvWBRigActivityCalc`, `wvWBFRigActivityCalc`

**Metrics:**
- `rop_by_formation` — drilling speed through each geologic layer
- `time_by_wellbore_section` — duration per hole section

### Priority 5 — Zone Production Entity (`semantic_model: wellview_zone_production`)

**Source calc tables:** `wvZoneProdTypDataCalc`

**Metrics:**
- `zone_oil_volume`, `zone_gas_volume`, `zone_water_volume`
- `zone_uptime_pct` — producing time / (producing + down time)

*Note: This overlaps with ProdView production data. The WellView version is zone-scoped; ProdView is unit-scoped. Use for validation and zone-level drilldowns.*

---

## Staging Priorities

The following calc tables should be staged first, prioritized by business value, data richness, and downstream model potential.

### Tier 1 — Stage First (Core KPIs)

| # | Calc Table | Domain | Grain | Why Stage First |
|---|---|---|---|---|
| 1 | `wvJCostCumCalc` | Cost | Job | Cumulative cost with AFE/field/invoice/forecast — the single most important cost table |
| 2 | `wvAFECalc` | Cost | Well | Cross-job AFE summary — enables well-level cost reporting without rebuilding from daily |
| 3 | `wvJTLSumCalc` | Time | Job | Time log summary with duration + % total — backbone of time analysis |
| 4 | `wvJRCostCalc` | Cost | Daily | Daily cost line items — feeds daily burn rate and cost trend analysis |
| 5 | `wvJRCostCumCalc` | Cost | Daily | Cumulative cost to date — feeds S-curve and budget tracking |
| 6 | `wvJRigActivityCalc` | Performance | Job | Sensor-derived rig activity with ROP, WOB, RPM — key drilling performance |
| 7 | `wvJRSafetyIncCalc` | Safety | Daily | Safety incident tracking — required for TRIR and HSE reporting |
| 8 | `wvJRSafetyChkCalc` | Safety | Daily | Safety check compliance — required for HSE dashboards |
| 9 | `wvJRIntervalProblemSumCalc` | NPT | Daily | NPT by type — feeds non-productive time analysis |
| 10 | `wvJPPActivitySumCalc` | Performance | Phase | Planned vs actual — feeds phase variance and schedule performance |

### Tier 2 — Stage Next (Enrichment)

| # | Calc Table | Domain | Grain | Why |
|---|---|---|---|---|
| 11 | `wvJDSSlideSheetCalc` | Performance | Drill String | Rotate vs slide analysis — directional drilling optimization |
| 12 | `wvJRStandCalc` | Performance | Daily | Stand KPIs — connection time benchmarking |
| 13 | `wvJPerformanceCalc` | Performance | Job/Month | Monthly drilling trends — BHA/bit runs, depth, hours |
| 14 | `wvWellboreSummaryCalc` | Construction | Wellbore | Wellbore section summary — feeds well construction reporting |
| 15 | `wvZoneProdTypDataCalc` | Production | Zone | Zone production volumes — validation against ProdView |
| 16 | `wvStimPropTypSumCalc` | Completions | Stimulation | Proppant summary — completions analytics |
| 17 | `wvJRPersCtSumCalc` | Personnel | Daily | Daily headcount — feeds personnel cost / efficiency |
| 18 | `wvWBFRigActivityCalc` | Construction | Formation | Rig activity by formation — geological performance |
| 19 | `wvJPPCostCalc` | Cost | Phase | Phase-level costs — feeds phase cost benchmarking |
| 20 | `wvAFECostSumCalc` | Cost | Well | Cumulative cost by AFE across jobs — complements `wvAFECalc` |

### Tier 3 — Stage Later (Deep Analysis)

Stage these when specific analytical use cases demand them:

- **Cost code drill-downs** (`wvJCostSumAFECode{1-6}Calc`, `wvJCostSumDailyCode{1-6}Calc`) — 12 tables for 6-level cost hierarchy
- **Vendor analysis** (`wvJVendorCalc`, `wvJVendorTicketNoCalc`, `wvJPONoCalc`) — vendor cost management
- **Phase type rollups** (`wvJPPCode1*Calc`) — 11 tables, useful for benchmarking by phase type
- **Rig crew analysis** (`wvJRRCCalc`, `wvJRRCRigActivityCalc`, `wvJRRCStandCalc`) — crew performance
- **Hydraulics** (`wvJDSDPHydCalc`, `wvJDSDPAVCalc`) — drilling engineering
- **Mud/fluids detail** (`wvJRMudAddCalc`, `wvJRMudVolCalc`, `wvJRFluidsCalc`) — materials management
- **Composite surveys** (`wvWCompDirSurveyCalc`, `wvWDSVSCalc`) — directional analysis

### Tables to Skip or Defer Indefinitely

- **`wvWellboreSumExcludeCalc`** — variant of `wvWellboreSummaryCalc` with exclusions; redundant unless specific filter needed
- **`wvJR30hrTimeLogCalc`** — extended 30-hour window; niche use case
- **`wvJROfflineTimeLogCalc`** — offline time log events; low value
- **`wvJRDetailComCalc`** — raw sensor time code interpretation; too granular for most analytics
- **Duplicate-grain rig activity detail tables** — each entity has both `*RigActivityCalc` (Type 1) and `*RigActivityDtlCalc` (Type 2); stage the Type 1 first, add Type 2 only when finer classification is needed

---

## Appendix: Full Calc Table Inventory

### By Source File

| Domain File | Calc Tables | Count |
|---|---|---|
| `operations_jobs.yaml` | All `wvJ*`, `wvJR*`, `wvJPP*`, `wvJDS*`, `wvJTL*`, `wvJStand*` tables | 127 |
| `wellbore_surveys.yaml` | `wvWellboreSummary*`, `wvWB*`, `wvWBF*`, `wvWComp*`, `wvWDS*` | 12 |
| `zones_completions.yaml` | `wvZone*Calc` | 3 |
| `perfs_stims.yaml` | `wvStim*Calc` | 2 |
| `other.yaml` | `wvAFE*Calc` | 2 |
| **Total** | | **146** |

*Note: 144 unique table names in YAML files; 2 entries appear twice (`wvJPPTLOpsCatCalc`, `wvJPPCode1TLOpsCatCalc`) across sections, bringing the document count to 146.*

### Rig Activity Calc Tables (Cross-Grain Pattern)

The same rig activity + stand KPI structure repeats at 7 grains — a key cross-cutting pattern:

| Grain | RigActivity (Type 1) | RigActivityDtl (Type 2) | Stand KPIs |
|---|---|---|---|
| Job | `wvJRigActivityCalc` | `wvJRigActivityDtlCalc` | — |
| Daily Report | `wvJRRigActivityCalc` | `wvJRRigActivityDtlCalc` | `wvJRStandCalc` |
| Rig Crew | `wvJRRCRigActivityCalc` | `wvJRRCRigActivityDtlCalc` | `wvJRRCStandCalc` |
| Phase | `wvJPPRigActivityCalc` | `wvJPPRigActivityDtlCalc` | `wvJPPStandCalc` |
| Drill String | `wvJDSRigActivityCalc` | `wvJDSRigActivityDtlCalc` | `wvJDSStandCalc` |
| Time Log Entry | `wvJTLRigActivityCalc` | `wvJTLRigActivityDtlCalc` | `wvJTLStandCalc` |
| Stand | `wvJStandRigActivityCalc` | `wvJStandRigActivityDtlCalc` | — |
| Wellbore | `wvWBRigActivityCalc` | `wvWBRigActivityDtlCalc` | `wvWBStandCalc` |
| Formation | `wvWBFRigActivityCalc` | `wvWBFRigActivityDtlCalc` | `wvWBFStandCalc` |

This pattern accounts for 27 tables (19% of all calc tables). Stage at Job and Daily Report grains first; add finer grains as analytical needs emerge.
