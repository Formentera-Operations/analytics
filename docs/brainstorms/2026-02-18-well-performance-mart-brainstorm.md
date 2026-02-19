# Well Performance Mart — Brainstorm

**Date:** 2026-02-18
**Author:** Rob Stover
**Status:** Ready for planning

---

## What We're Building

A cross-domain well performance data asset combining:

| Source | Data | Grain |
|--------|------|-------|
| ProdView | Production volumes (oil, gas, water, BOE) | Well-Month |
| ODA (`fct_los`) | Revenue, LOE, severance tax, net income | Well-Month |
| ODA (`bridge_job_afe`, `dim_afes`) | AFE authorization + actual capex | Per Job/Well |
| WellView (`fct_daily_drilling_cost`) | D&C cost per job → well | Per Job/Well |
| WellView (`fct_drilling_time`) | Drilling + NPT hours by phase | Per Job/Well |
| WellView (`fct_stimulation`) | Proppant, stages, fluid volumes | Per Job/Well |
| WellView (`fct_npt_events`) | NPT event count + hours | Per Job/Well |
| `well_360` | Well identity, API numbers, basin/unit/formation | Well Dimension |

**Output models:**

- `fct_well_performance_monthly` — well-month grain: production volumes + LOS financials
- `fct_well_performance_summary` — well-lifetime grain: rolled-up production + financials + D&C context

**Consumers:** Operations leadership (well scorecards, underperformance flags), Finance team (LOS reconciliation, AFE vs actual capex), Engineering (completion quality vs production correlation).

---

## Why This Approach

**Intermediate-first architecture (Approach B)** — 4 pre-aggregating intermediates + 2 mart models.

Rationale:
1. **WellView→EID join is non-trivial.** WellView jobs link to wellbores (`dim_wellbore`) which link to API numbers, which then need to join back to `well_360` via `api_10`/`api_14`. This deserves its own tested step rather than inline logic in a mart.
2. **Grain mismatch must be resolved before the final join.** ProdView/LOS are time-series (well-month); WellView is event-based (per job). Each source needs to be collapsed to EID grain before the summary model can JOIN them.
3. **Each intermediate is independently testable.** We can validate BOE calculations, LOS roll-ups, and D&C cost per EID without running the full pipeline.
4. **Mirrors existing pattern.** `int_wellview__daily_drilling_enriched` → `fct_daily_drilling_cost` already uses this model; we're building parallel intermediates at the EID roll-up level.

---

## Key Decisions

### 1. Well Identity Bridge
- **EID is the join key** across all sources
- `well_360` is the authoritative EID→API mapping

**Per-source EID resolution (validated by reading models):**

| Source | EID Path | Notes |
|--------|----------|-------|
| `fct_los` | `right(well_code, 6)` | ODA cost center encodes EID as last 6 chars (e.g., `FO012345` → `012345`). String derivation — no join needed. |
| `int_prodview__production_volumes` | `id_rec_unit` → `stg_prodview__units.api_10` → `well_360.api_10` | Must join through units table to get API-10, then to well_360. This is the only non-trivial identity join. |
| `fct_daily_drilling_cost` | Column `eid` already present | Resolved inline via `well_360` join in the mart. |
| `fct_drilling_time` | Column `eid` already present | Resolved inline via `well_360` join in the mart. |
| `fct_stimulation` | Column `eid` already present | Resolved inline via `well_360` join in the mart. |

### 2. Intermediate Models

| Model | Grain | Source(s) | Key Logic |
|-------|-------|-----------|-----------|
| `int_well_perf__prodview_monthly` | EID + month | `stg_prodview__daily_allocations`, `stg_prodview__units` | Read directly from staging (NOT via `int_prodview__production_volumes` — that model is a legacy quoted-alias compatibility bridge). Join `id_rec_unit` → `stg_prodview__units.api_10` → `well_360.api_10` for EID. Sum volumes to month. |
| `int_well_perf__los_monthly` | EID + month | `fct_los` | Derive EID as `right(well_code, 6)`. Pass through `los_gross_amount`/`los_net_amount` by los_category. Filter: `location_type = 'Well'` only. |
| `int_well_perf__drilling_summary` | EID | `fct_daily_drilling_cost`, `fct_drilling_time`, `fct_npt_events` | All three already carry `eid`. SUM cost + hours per EID across all jobs. |
| `int_well_perf__completion_summary` | EID | `fct_stimulation` | Already carries `eid`. SUM stages, proppant, fluid volumes per EID (multiple stim jobs per well possible). |

### 3. Mart Models

**`fct_well_performance_monthly`**
- Grain: `(eid, production_month)`
- Spine: `int_well_perf__prodview_monthly` (production is the activity driver)
- LEFT JOIN: `int_well_perf__los_monthly` — months with no LOS entry get NULLs (expected for inactive wells)
- Columns: EID, month, oil_bbls, gas_mcf, water_bbls, boe, revenue, loe, severance_tax, net_income
- Materialization: **Table** (time-series, ~500K–2M rows estimated)
- Clustering: `(eid, production_month)`

**`fct_well_performance_summary`**
- Grain: `eid` (one row per well, lifetime)
- Spine: `well_360` (all known EIDs, even pre-production)
- Aggregations from monthly fact: total BOE, peak month production, producing months count, cumulative revenue, cumulative LOE
- LEFT JOINs: `int_well_perf__drilling_summary`, `int_well_perf__completion_summary`
- Columns: all well_360 identity fields + production lifetime + financial lifetime + D&C context
- Materialization: **Table** (~9K–15K rows)

### 4. Materialization Strategy
- Intermediates: `ephemeral` (no need to materialize pre-aggregations; they're purpose-built for these two marts)
- `fct_well_performance_monthly`: **table** — large time-series, queried with date filters; cluster by (eid, production_month)
- `fct_well_performance_summary`: **table** — small enough to rebuild fast; acts as the scorecard spine

### 5. Production Source Priority
- **ProdView only** for this phase — Griffin has overlap but adds complexity
- If Griffin coverage expands, revisit with a `COALESCE(prodview, griffin)` priority pattern

### 6. Financial Data Scope (Phase 1)
- LOS + AFE/capital spending (via drilling summary)
- AR Aging excluded for now — receivables is a separate finance use case
- AP/vendor spend excluded — join path to well level is expensive and ODA vendor spend isn't reliably well-coded

---

## Open Questions

One remaining question to validate before sprint planning:

1. **What's the date range for LOS data?** Run `dbt show --select fct_los --limit 5` and check earliest/latest `journal_date`. If LOS starts after 2018, monthly fact will have NULL financial windows for older production history.

All other open questions resolved (see Well Identity Bridge section above).

---

## Non-Goals (Phase 1)

- No ComboCurve forecast columns (forecast vs actual is a Phase 2 extension)
- No Griffin production data
- No AR/AP financial data at well level
- No partner-tenant (FP) version yet — Operations (FO) only
- No real-time or near-real-time refresh — daily batch is sufficient

---

## Architecture Evolution (Post-Brainstorm Decisions)

### Gold Layer = True Galaxy Schema

After further discussion, the gold mart layer should be a **galaxy (multi-star) schema** — one
fact per business process, all sharing `well_360` as the conformed dimension. No cross-process
fact tables in gold.

**`fct_well_performance_monthly` is removed from the plan.** It was combining two independent
business processes (production volumes + LOS financials) into one fact — that's the platinum
layer's job, not gold.

Revised gold facts:
- `fct_well_production_monthly` — production volumes at well-month grain (source-agnostic name)
- `fct_los` — stays as-is (GL transaction grain, already exists)
- WellView drilling facts — already exist

The LOS monthly rollup (`int_los__well_monthly`) is an ephemeral intermediate that feeds
platinum only. If direct consumers emerge, graduate it to a gold fact.

### Source Naming Removed from Fact Models

`fct_prodview__production_monthly` → renamed `fct_well_production_monthly`. Facts represent
business entities, not source systems. If a second production source is acquired, the staging
or intermediate layer handles the union — the fact contract stays stable.

### Platinum Layer = Cross-Process OBT

`plat_well__performance_scorecard` joins:
- `well_360` identity (denormalized — all attributes embedded)
- Rolled-up production from `fct_well_production_monthly`
- Rolled-up financials from `fct_los` via `int_los__well_monthly`
- D&C context from WellView drilling facts

### well_360 Must Become the Canonical Well Dimension

`dim_wells` (finance/LOS-era ODA-only well dimension) has zero downstream SQL consumers
and should be deprecated. Its derived logic (basin classification, `is_revenue_generating`,
ODA billing/revenue flags, `well_type` from name patterns) must be migrated into `well_360`
before platinum layer work begins.

**Separate sprint planned:** `docs/plans/2026-02-19-feat-well-360-canonical-dim-evolution-plan.md`

**Sprint sequencing:**
1. `well_360` canonical dim evolution (separate sprint, prerequisite)
2. `fct_well_production_monthly` + `int_los__well_monthly` (gold foundation)
3. `plat_well__performance_scorecard` (platinum OBT, joins gold + well_360)

---

## Sprint Sketch (Original — superseded by architecture evolution above)

**Sprint 1 (Foundation):** Validate join keys across all sources; build 4 ephemeral intermediates; validate row counts and BOE calculations.

**Sprint 2 (Mart Models):** Build `fct_well_performance_monthly` + `fct_well_performance_summary`; write YAML docs + tests (not_null, relationships to well_360); validate against known wells.

**Sprint 3 (Extensions):** Add forecast vs actual columns from ComboCurve; add Griffin supplement pattern if needed.
