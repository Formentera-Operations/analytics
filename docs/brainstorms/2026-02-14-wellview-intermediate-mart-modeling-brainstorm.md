# Brainstorm: WellView Intermediate & Mart Modeling Sprint

**Date:** 2026-02-14
**Status:** Ready for planning
**Next step:** `/workflows:plan` for Sprint 1 (fct_daily_drilling_cost)

---

## What We're Building

Turn 58 validated WellView staging models into business-facing marts — dimensions and facts that answer "What did we drill?", "What did it cost?", "How did we perform?", and "What equipment is downhole?"

The entity model (`context/sources/wellview/entity_model.md`) defines two core models:
- **Physical Well** — asset-centric dimensions (equipment, surveys, cement, completions)
- **Well Work** — event-centric facts (costs, time, NPT, stimulation, drilling performance)

Sprint 1 dimensions (dim_job, dim_wellbore, dim_phase, bridge_job_afe) are already built and merged. This brainstorm covers everything that comes next.

---

## Why This Approach

### Value-Ordered Singles

Five focused sprints, each scoped to fit in a single agent context window. Ship and validate sequentially. Each sprint produces 1-3 models (intermediate if needed + mart + tests).

**Why not parallel tracks?** Cross-sprint dependencies exist (equipment dim may be needed by drilling facts later), and sequential validation catches issues before they cascade. Agent-team parallelism happens *within* a sprint (multiple models), not *across* sprints.

**Why not big-bang?** Too large for one context window. 10+ models with cross-dependencies can't be reliably validated atomically.

### Intermediates Only When Justified

Per CLAUDE.md, create intermediates only when:
1. **Multi-source join** — 2+ staging models combined (e.g., costs + reports + jobs)
2. **Reusable logic** — transformation needed by 2+ marts
3. **Complex classification** — business rules that shouldn't live in mart CTEs

Existing drilling dims (dim_job, dim_wellbore) source directly from staging — this pattern continues for simple marts.

### Materialization Strategy

Per CLAUDE.md:
- **Ephemeral** (default) — most intermediates
- **Table** — expensive logic referenced by 3+ downstream models
- **Incremental** — datasets too large for full rebuild (daily costs = 1.9M rows)
- **View** — only if intermediate needs direct query access for debugging

---

## Sprint Plan

### Sprint 1: `fct_daily_drilling_cost` (Highest Business Value)

**Question answered:** "What did we spend, when, on what, and is it tracking to budget?"

**Models:**

| Model | Type | Materialization | Why |
|-------|------|----------------|-----|
| `int_wellview__daily_cost_enriched` | Intermediate | ephemeral | Multi-source join: costs + reports (for date) + jobs (for category). Adds well_360 FK. Used by fact. |
| `fct_daily_drilling_cost` | Mart/Fact | **incremental** (merge on cost_line_id, watermark on `_loaded_at`) | 1.9M rows, grows daily. Grain: one row per cost line item. |

**Intermediate justification:** The cost staging model has `job_report_id` but no report date or job context. Joining costs → reports → jobs is a 3-model chain that shouldn't live in mart CTEs and may be reused by cost-related metrics later.

**Key design decisions:**
- **Recurring costs excluded initially.** Only 485 rows / 20 wells. Add via UNION later if business asks.
- **Account hierarchy:** 6-level code hierarchy (main_account_id through afe_category) passes through. No reclassification in this sprint — that's an ODA concern.
- **AFE linkage:** Join to `bridge_job_afe` for budget vs actual. Cross-system ODA AFE bridge is a future sprint.
- **Cluster by:** `well_id, job_id` for common query patterns.

**Validation:** Job-level cost sums for HOOEY N731H must match entity model ($5.0M drilling, $5.4M completion, $1.3M facilities, $15K servicing = ~$11.8M total).

**Agent context needed:**
- `context/sources/wellview/entity_model.md` (Cost entity section)
- `stg_wellview__daily_costs` (SQL + YAML)
- `stg_wellview__job_reports` (SQL + YAML)
- `stg_wellview__jobs` (SQL + YAML)
- `dim_job.sql` (existing mart pattern reference)
- `well_360.sql` (FK reference)
- CLAUDE.md incremental model pattern

---

### Sprint 2: `fct_drilling_time` + `fct_npt_events`

**Questions answered:** "How did we spend our time?" and "What went wrong?"

**Models:**

| Model | Type | Materialization | Why |
|-------|------|----------------|-----|
| `fct_drilling_time` | Mart/Fact | table | 762K rows. Grain: one row per time log entry. Includes report_date from staging (job_report_id calc column). |
| `fct_npt_events` | Mart/Fact | table | 14K rows. Grain: one row per NPT event. Small enough for full rebuild. |

**No intermediates needed.** Time log staging already has `job_report_id` (calc column) for date context, `job_id` for job FK, `well_id` for well FK. Single-source facts.

**Key design decisions:**
- **NPT dual-source:** Time logs have `is_problem_time` flag; interval problems have detailed categorization. `fct_npt_events` sources from interval problems (richer). `fct_drilling_time` carries the boolean flag for high-level NPT %.
- **Time validation:** Drilling time entries for HOOEY sum to 678.5 hrs / ~28.3 days — should roughly match job duration (31 report days minus non-drilling time).
- **Activity code hierarchy:** `time_log_code_1` (FRAC, FLOWBACK, PRODUCTION DRILLING, etc.) and `time_log_code_2` pass through as-is. No reclassification.

**Validation:** HOOEY NPT: 3 events, 9.0 gross hours, all "rig equip / Surface Equipment".

---

### Sprint 3: `dim_well_equipment` (Physical Well)

**Question answered:** "What's downhole, what's the lift type, and what's the equipment history?"

**Models:**

| Model | Type | Materialization | Why |
|-------|------|----------------|-----|
| `int_wellview__equipment_unified` | Intermediate | **table** | Complex COALESCE across 4+ staging sources. Artificial lift inference logic. Referenced by dim + potentially well_360 refresh. |
| `dim_well_equipment` | Mart/Dim | table | One row per well with current equipment snapshot + lift type. ~4,000 wells. |

**Intermediate justification:** Artificial lift type requires COALESCE priority across 3 sources:
1. `PRODMETHTYP` from `stg_wellview__production_settings` (~670 wells)
2. Equipment inference from rods/tubing descriptions (~3,984 wells)
3. `setting_objective` as tertiary signal

This logic is complex and reusable — `well_360` may eventually want lift type as an attribute. Table materialization because it's referenced by 2+ consumers and involves cross-table logic.

**Key design decisions:**
- **Lift type COALESCE:** Priority order validated against KING 59 1 (ESP) and JB Tubb AC 1 #504 (Rod Pump).
- **Current-state focus:** `dim_well_equipment` shows *current* equipment (latest in-hole run). Historical run/pull lifecycle is a future `fct_equipment_history` fact.
- **Component-level detail deferred.** 194K component records enable BOM/failure analysis but are Sprint 5+ material.

**Validation:** Portfolio lift type distribution: ~1,886 rod pump, ~73 ESP, ~2,025 other.

---

### Sprint 4: `fct_stimulation`

**Question answered:** "How was the well stimulated — stages, clusters, proppant, cost?"

**Models:**

| Model | Type | Materialization | Why |
|-------|------|----------------|-----|
| `fct_stimulation` | Mart/Fact | table | Grain: one row per stimulation event with stage/cluster/proppant aggregations. Sources from 8 stim staging models via CTEs. |

**No intermediate needed.** The stim hierarchy is well-structured (stim → stages → fluids → proppants) and all staging models share `well_id` + hierarchical IDs. CTE joins within the mart are clean.

**Key design decisions:**
- **Grain options:** One row per stim event (summary) vs one row per stage. Start with per-stim summary. Per-stage detail is a future `fct_stimulation_stages` fact.
- **Dual membership:** Stim is both Physical Well (geometry) and Well Work (cost/schedule). The mart includes both facets in one model. No need to split.
- **Perf coupling:** Include perf count/shot count as stim attributes (512 perfs = 512 clusters for HOOEY). Join to perfs staging on well_id + depth overlap or stim calc table.

**Validation:** HOOEY: 1 Sand Frac, 64 stages, 512 clusters, $4.7M, Liberty Energy.

---

### Sprint 5: `dim_well_survey`

**Question answered:** "What's the well's 3D trajectory?"

**Models:**

| Model | Type | Materialization | Why |
|-------|------|----------------|-----|
| `dim_well_survey` | Mart/Dim | table | One row per survey station with trajectory calculations. 363K rows. |

**No intermediate needed.** Direct join of `stg_wellview__wellbore_directional_surveys` → `stg_wellview__wellbore_directional_survey_data`. Simple parent-child.

**Key design decisions:**
- **Grain:** One row per survey station (MD/incl/azimuth/TVD). Survey metadata (type, date, use_for_calculations) denormalized onto each station row.
- **Filter to actual:** `proposed_or_actual = 'actual'` only. Proposed surveys are planning artifacts.
- **Wellbore FK:** Link to `dim_wellbore` via wellbore_id.

**Validation:** HOOEY: 1 MWD Survey, 201 stations, 222–19,777 ft, horizontal turn profile confirmed (0.4° → 92° inclination).

---

## Key Decisions

1. **5 value-ordered sprints**, each agent-assignable and context-window-scoped
2. **Intermediates only when justified** — Sprints 1 and 3 get intermediates; Sprints 2, 4, 5 go staging → mart
3. **Incremental only for daily costs** (1.9M rows). Everything else is table (full rebuild feasible)
4. **Recurring costs excluded** from Sprint 1 (485 rows / 20 wells — add later if asked)
5. **Existing dims untouched** — dim_job, dim_wellbore, dim_phase, bridge_job_afe stay as-is
6. **Data quality handling:** Cement negative durations (source data error) — NULLify in intermediate/mart via `nullif(duration_hours < 0, true)` pattern. Don't fix source data.
7. **AFE cross-system bridge deferred.** Sprint 1 cost fact stays WellView-only. Future sprint creates `int_drilling__afe_enriched` joining `bridge_job_afe.afe_number` = `dim_afes.afe_code` (simple string match). ODA AFE brings project_category, financial_impact_type, and budget authorization amounts.
8. **Directory structure resolved:**
   - New marts → `models/operations/marts/drilling/` (alongside existing dim_job, dim_wellbore, dim_phase, bridge_job_afe)
   - New intermediates → `models/operations/intermediate/drilling/` (new directory)
   - Follows existing domain-based organization pattern (drilling, finance, production, griffin)
9. **Intermediate materialization:**
   - `int_wellview__daily_cost_enriched` → **ephemeral** (compiles as CTE into fact, no Snowflake object, debug via `dbt show`)
   - `int_wellview__equipment_unified` → **table** (complex cross-source COALESCE, potentially reused by well_360)

## Resolved Open Questions

| Question | Decision | Rationale |
|----------|----------|-----------|
| AFE cross-system bridge timing | **Defer to future sprint** | Keep Sprint 1 scope tight for agent context. Bridge is a simple `afe_number = afe_code` string match — can add anytime. |
| Directory structure | **marts/drilling/ + intermediate/drilling/ (new)** | Domain-based organization matches existing pattern (finance/, production/, griffin/). All WellView drilling models in one place. |
| int_wellview__daily_cost_enriched materialization | **Ephemeral** | Single consumer (fct_daily_drilling_cost). No debugging advantage over `dbt show`. Follows CLAUDE.md default. |
| int_wellview__equipment_unified materialization | **Table** | Complex cross-source logic, potentially 2+ consumers (dim + well_360). Expensive to recompute. |

## Remaining Open Questions

1. **Equipment lift type in well_360:** Should Sprint 3's lift type inference feed back into `well_360`? Deferred until Sprint 3 planning — decide based on whether well_360 consumers need lift type.
2. **Stimulation per-stage grain:** Sprint 4 starts with per-stim summary. Per-stage detail (`fct_stimulation_stages`) deferred until business asks for stage-level analytics.

---

## Intermediate Layer Design Detail

### `int_wellview__daily_cost_enriched` (Sprint 1)

**Why it exists:** Cost staging has `job_report_id` and `well_id` but no report date, job category, or well_360 FK. The fact needs all three. This is a 3-model join chain (costs → reports → jobs) plus well_360 enrichment.

```
Location:     intermediate/drilling/int_wellview__daily_cost_enriched.sql
Materialized: ephemeral
Grain:        1 row per cost line item (same as stg_wellview__daily_costs)
Sources:      stg_wellview__daily_costs
              + stg_wellview__job_reports (for report_date, report_number)
              + stg_wellview__jobs (for job_category, job_type_primary)
              + well_360 (for well_sk, eid)
Columns added:
  - report_date (from job_reports)
  - report_number (from job_reports)
  - job_category (from jobs — Drilling/Completion/Facilities/Well Servicing)
  - job_type_primary (from jobs)
  - well_sk (from well_360 — FK for joining to well dimension)
  - eid (from well_360 — 6-char Formentera well ID)
  - job_sk (from dim_job — FK for joining to job dimension)
```

### `int_wellview__equipment_unified` (Sprint 3)

**Why it exists:** Artificial lift type inference requires COALESCE across 4+ staging sources with complex classification logic. Reusable by dim_well_equipment and potentially well_360.

```
Location:     intermediate/drilling/int_wellview__equipment_unified.sql
Materialized: table
Grain:        1 row per well
Sources:      stg_wellview__tubing_strings (current in-hole, description for ESP detection)
              + stg_wellview__rod_strings (current in-hole, presence = rod pump)
              + stg_wellview__production_settings (PRODMETHTYP when populated)
              + well_360 (well_sk, eid)
Columns added / derived:
  - lift_type_source (which source determined the lift type: 'prod_settings', 'equipment_inference', 'setting_objective')
  - inferred_lift_type ('Rod Pump', 'ESP', 'Gas Lift', 'Plunger', 'Flowing', 'Unknown')
  - has_rod_strings (boolean)
  - has_tubing_strings (boolean)
  - current_rod_string_count (integer)
  - current_tubing_string_count (integer)
  - latest_rod_install_date (timestamp)
  - latest_tubing_install_date (timestamp)
  - well_sk, eid (from well_360)
```

---

## Ground-Truth Validation Data (HOOEY N731H)

All entity data validated against HOOEY-STX-UNIT N731H (EID `109181`, WellView ID `FF3A6D0E6DB24DF1A7CC596EA4119A95`).

### Validated Entity Volumes

| Entity | HOOEY Value | Portfolio Total |
|--------|------------|----------------|
| Daily costs | 2,123 lines, $11.8M | 1.9M rows, 5,229 wells |
| Time log entries | 574 entries, 2,599 hrs | 762K rows, 4,542 wells |
| NPT events | 3 events, 9.0 hrs | 14K rows, 1,429 wells |
| Job reports | 156 reports | 245K rows, 5,308 wells |
| Drill strings (BHA) | 7 actual runs | 14K rows, 2,114 wells |
| Cement activities | 3 jobs | 10K rows, 3,340 wells |
| Dir survey stations | 201 stations | 363K rows, 2,508 wells |

### HOOEY Cost Breakdown by Job

| Job Category | Cost Lines | Field Estimate | Vendors | Report Days |
|-------------|-----------|---------------|---------|-------------|
| Drilling | 1,210 | $5,026,886 | 77 | 31 |
| Completion | 763 | $5,401,970 | 15 | 76 |
| Facilities | 141 | $1,310,729 | 35 | 44 |
| Well Servicing | 9 | $14,950 | 2 | 5 |

### Data Quality Issues Found

| Issue | Entity | Detail | Handling |
|-------|--------|--------|----------|
| Negative duration | Cement activities | Production cement end_date (2024-11-16) < start_date (2025-06-12) = -4,978 hrs | NULLIF negative durations in mart |
| Null BHA result | Drill strings | BHA #3 (Int section) has null result | Allow NULL — not all BHA runs get classified |
| Null time_log_code_1 | Time logs | 8 Completion entries with null code_1, 0 duration | Filter out zero-duration entries in mart |

---

## Context Files for Agent Tasks

Each sprint's agent should load these files:

| File | Purpose |
|------|---------|
| `context/sources/wellview/entity_model.md` | Entity definitions, relationships, validation data |
| `context/sources/wellview/wellview.md` | Source system overview, join patterns, gotchas |
| `CLAUDE.md` | Staging/intermediate/mart patterns, materialization rules |
| `models/operations/marts/drilling/dim_job.sql` | Reference mart pattern (surrogate keys, well_360 join) |
| `docs/brainstorms/2026-02-14-wellview-intermediate-mart-modeling-brainstorm.md` | This document — sprint specs |
| Sprint-specific staging SQL + YAML | Column names, types, source tables |

### Sprint-Specific Staging Models

| Sprint | Staging Models to Load |
|--------|----------------------|
| S1: Cost | `stg_wellview__daily_costs`, `stg_wellview__job_reports`, `stg_wellview__jobs` |
| S2: Time/NPT | `stg_wellview__job_time_log`, `stg_wellview__job_interval_problems` |
| S3: Equipment | `stg_wellview__tubing_strings`, `stg_wellview__rod_strings`, `stg_wellview__production_settings` |
| S4: Stimulation | `stg_wellview__stimulations`, `stg_wellview__stimulation_stages`, `stg_wellview__stimulation_stage_fluids`, `stg_wellview__stimulation_stage_proppants`, `stg_wellview__perforations` |
| S5: Survey | `stg_wellview__wellbore_directional_surveys`, `stg_wellview__wellbore_directional_survey_data` |
