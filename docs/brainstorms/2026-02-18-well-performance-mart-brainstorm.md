# Well Performance Mart — Brainstorm

**Date:** 2026-02-18
**Author:** Rob Stover
**Status:** Architecture finalized — see sprint plans to implement

---

## What We're Building

A cross-domain well performance data asset combining production, financial, and D&C context
across a **galaxy (multi-star) schema** in gold, with a denormalized OBT in platinum for
BI consumption.

**Output models (final):**

| Model | Layer | Grain | Purpose |
|-------|-------|-------|---------|
| `fct_well_production_monthly` | Gold (marts) | EID + month | Production volumes — source-agnostic fact |
| `int_los__well_monthly` | Intermediate (ephemeral) | EID + month | LOS financials rolled up from `fct_los`, feeds platinum only |
| `int_well_perf__drilling_summary` | Intermediate (ephemeral) | EID | D&C cost + time + NPT per well, feeds platinum only |
| `int_well_perf__completion_summary` | Intermediate (ephemeral) | EID | Stim metrics per well, feeds platinum only |
| `plat_well__performance_scorecard` | Platinum | EID | Denormalized OBT — `well_360` identity + all rolled-up metrics |

**Prerequisite sprint (must run first):**
Evolve `well_360` into the canonical well dimension — absorb `dim_wells` logic, add
`oda_well_id`, `basin_name`, revenue flags.
See `docs/plans/2026-02-19-feat-well-360-canonical-dim-evolution-plan.md`.

**Consumers:** Operations leadership (well scorecards, underperformance flags),
Finance team (LOS reconciliation, AFE vs actual capex), Engineering
(completion quality vs production correlation).

---

## Architecture Decisions

### Gold = True Galaxy (Multi-Star) Schema

One fact per business process, all sharing `well_360` as the conformed dimension.
Facts do not know about each other. Cross-process combination happens in platinum only.

```
well_360 (conformed dimension)
  ├── fct_well_production_monthly    ← NEW: production business process
  ├── fct_los                        ← EXISTING: financial/GL business process
  ├── fct_daily_drilling_cost        ← EXISTING
  ├── fct_drilling_time              ← EXISTING
  └── fct_stimulation                ← EXISTING
```

**Why not a combined `fct_well_performance_monthly`?**
Production (ProdView) and financials (ODA GL) are independent business processes with
different owners, refresh cadences, and governance. Combining them in gold conflates
those concerns. The platinum layer exists precisely to do cross-process aggregation
for consumption — that's its job.

### Fact Naming = Business Entity, Not Source System

`fct_well_production_monthly`, not `fct_prodview__production_monthly`. If a second
production source is acquired, the staging/intermediate layer handles the union.
The fact contract (columns, grain, semantics) stays stable.

### Platinum = Cross-Process OBT

`plat_well__performance_scorecard` is the one-stop-shop for well performance analysis.
It is fully denormalized — all `well_360` identity attributes embedded directly.
No joins required at query time.

### well_360 is the Canonical Well Dimension

`dim_wells` (ODA-only, finance-era) has zero downstream SQL consumers and is deprecated.
Its derived logic migrates into `well_360`. After the prerequisite sprint, `dim_wells`
is deleted.

---

## Source Inventory

| Source | Data | Grain | Layer |
|--------|------|-------|-------|
| ProdView (`stg_prodview__daily_allocations`) | Production volumes | Daily → roll to month | Staging → Gold |
| ODA (`fct_los`) | Revenue, LOE, severance tax | GL transaction | Gold (existing) |
| WellView (`fct_daily_drilling_cost`) | D&C cost per job | Per cost line | Gold (existing) |
| WellView (`fct_drilling_time`) | Drilling + NPT hours | Per time log | Gold (existing) |
| WellView (`fct_stimulation`) | Proppant, stages, fluid | Per stim job | Gold (existing) |
| WellView (`fct_npt_events`) | NPT event count + hours | Per event | Gold (existing) |
| `well_360` | Well identity, basin, API numbers | Per well (EID) | Gold dimension |

---

## EID Join Strategy (Validated)

All join paths validated by querying Snowflake directly (2026-02-18).

| Source | EID Resolution | Notes |
|--------|----------------|-------|
| `fct_well_production_monthly` | Two-step COALESCE: `id_rec_unit` → `well_360.prodview_unit_id` first, `stg_prodview__units.api_10` → `well_360.api_10` fallback | 81.5% match on pvunitcomp units. Filter `unit_type = 'pvunitcomp'` first to exclude facilities/externals. |
| `int_los__well_monthly` | `right(well_code, 6)` | ODA cost center encodes EID as last 6 chars — string derivation, no join. Filter `location_type = 'Well'` only. |
| `fct_daily_drilling_cost` | `eid` column already present | Resolved inline in the mart. |
| `fct_drilling_time` | `eid` column already present | Resolved inline in the mart. |
| `fct_stimulation` | `eid` column already present | Resolved inline in the mart. |
| `fct_npt_events` | `eid` column already present | Resolved inline in the mart. |

**ProdView EID gap (~18.5% of pvunitcomp):**
- Non-operated wells (~820): expected — not in ODA/WellView spine
- Injection/SWD (~145): not production wells
- Operated Gas Wells (~1,145): FP Griffin wells migrated to ProdView without
  `property_eid` backfilled. Resolve automatically when EIDs are added.
- **Decision:** include ALL pvunitcomp rows with `is_eid_unresolved` flag — do not drop.

---

## Model Specs

### `fct_well_production_monthly` (gold, table)

- **Grain:** `(eid, production_month)`
- **Spine:** `stg_prodview__daily_allocations` filtered to `pvunitcomp` units
- **EID join:** two-step COALESCE (prodview_unit_id → api_10)
- **Unique key:** `generate_surrogate_key(['eid', 'production_month'])`
- **Clustering:** `(eid, production_month)`
- **Key columns:** `eid`, `id_rec_unit`, `production_month`, `is_eid_unresolved`,
  `oil_bbls`, `gas_mcf`, `water_bbls`, `ngl_bbls`, `gross_boe`
- **EID is the only well identity column** — join to `well_360` at query time for attributes
- **Tags:** `['marts', 'fo', 'well_360']`

### `int_los__well_monthly` (ephemeral, feeds platinum only)

- **Grain:** `(eid, los_month)`
- **Source:** `fct_los` — filter `location_type = 'Well'`
- **EID:** `right(well_code, 6)`
- **Logic:** `SUM(CASE WHEN los_category = ... THEN los_gross_amount ELSE 0 END)` pivot
- **Key columns:** `eid`, `los_month`, `los_revenue`, `los_loe`, `los_severance_tax`, `los_net_income`
- Check actual `los_category` values with `dbt show` before writing CASE statements

### `int_well_perf__drilling_summary` (ephemeral, feeds platinum only)

- **Grain:** `eid`
- **Sources:** `fct_daily_drilling_cost`, `fct_drilling_time`, `fct_npt_events`
- **Logic:** SUM across all jobs per EID (all three already carry `eid`)
- **Key columns:** `eid`, `total_dc_cost`, `total_drilling_hours`, `total_npt_hours`,
  `npt_pct`, `job_count`

### `int_well_perf__completion_summary` (ephemeral, feeds platinum only)

- **Grain:** `eid`
- **Source:** `fct_stimulation` (already carries `eid`)
- **Logic:** SUM across all stim jobs per well
- **Key columns:** `eid`, `total_stages`, `total_proppant_lb`, `total_clean_volume_bbl`,
  `lateral_length_ft`, `proppant_per_ft_lb`

### `plat_well__performance_scorecard` (platinum, table)

- **Grain:** `eid` — one row per well
- **Spine:** `well_360` (all EIDs including pre-production)
- **Denormalized:** all `well_360` identity columns embedded directly — no joins at query time
- **LEFT JOINs:** all four intermediates/facts above
- **Aggregates from `fct_well_production_monthly`:** cumulative + peak production, first/latest month
- **Aggregates from `int_los__well_monthly`:** cumulative revenue, LOE, net income
- **From `int_well_perf__drilling_summary`:** total D&C cost, hours, NPT pct, `has_drilling_data` flag
- **From `int_well_perf__completion_summary`:** stim metrics, `has_completion_data` flag
- **Tags:** `['platinum', 'fo', 'well_360']`

---

## Non-Goals (Phase 1)

- No ComboCurve forecast vs actual columns (Phase 2 extension)
- No Griffin production data (Phase 2 when Griffin→ProdView migration completes)
- No AR/AP financial data at well level
- No partner-tenant (FP) version — Operations (FO) only
- No real-time refresh — daily batch sufficient
- No SCD2 history on `well_360`
- `fct_los` transaction grain stays as-is — no new gold monthly fact unless direct consumers emerge

---

## Open Questions

1. **LOS date range:** Validate earliest/latest `journal_date` in `fct_los` before building
   `int_los__well_monthly`. If LOS starts after 2018, platinum scorecard will have NULL
   financial windows for older production history — document the cutoff.

---

## Sprint Sequencing

**Sprint 0 (prerequisite):** `well_360` canonical dim evolution
→ `docs/plans/2026-02-19-feat-well-360-canonical-dim-evolution-plan.md`

**Sprint 1 (gold foundation):** `fct_well_production_monthly`
→ Update `docs/plans/2026-02-18-feat-well-performance-mart-plan.md` before running

**Sprint 2 (platinum OBT):** `int_los__well_monthly` + `int_well_perf__drilling_summary`
+ `int_well_perf__completion_summary` + `plat_well__performance_scorecard`
