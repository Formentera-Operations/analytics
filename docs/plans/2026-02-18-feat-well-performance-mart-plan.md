# Well Performance Mart — Implementation Plan

**Date:** 2026-02-18
**Branch:** `feature/well-performance-mart`
**Status:** READY — EID investigation complete, gap identified and scoped
**Brainstorm:** `docs/brainstorms/2026-02-18-well-performance-mart-brainstorm.md`

---

## Blocker: ProdView EID Investigation — ✅ RESOLVED (2026-02-18)

**Findings:**

Total ProdView units: 10,182. Of these, ~3,000 are `facility`, `external out`, `external in`
(batteries, meters, sales points) — they should NEVER join to well_360. Filter to
`unit_type = 'pvunitcomp'` only (7,217 rows) before any join.

**Match rates for pvunitcomp (actual wells) against well_360:**

| Join Path | Matched | Rate |
|-----------|---------|------|
| `prodview_unit_id` only | 4,430 | 61.4% |
| `api_10` only | 5,655 | 78.4% |
| Combined (prodview_unit_id OR api_10) | 5,883 | **81.5%** |

**Unmatched 18.5% breakdown:**
- Non-Operated wells (is_operated=False): ~860 — expected, not in ODA/WellView spine
- Injection/SWD/Water Source wells: ~145 — not production wells, can be excluded
- **Operated Gas Wells: ~1,145 — FP Griffin wells (see below)**

**Root cause of the ~1,145 operated Gas Wells gap:**
These are Formentera Partners (FP) wells that were migrated from Griffin into ProdView
but have not yet had their EIDs (`property_eid` / `unitidpa`) backfilled. The signature
is unmistakable: `is_operated=True`, valid `api_10`, `unit_sub_type = 'Gas Well'`, but
`property_eid IS NULL` and no match in `well_360` (which is built from the FO ODA spine).

These wells will automatically resolve as EIDs are added in ProdView — the two-step
COALESCE join will pick them up without any model changes. The `is_eid_unresolved` flag
serves as a real-time indicator of how many FP Griffin wells are still pending onboarding.

**Recommended join pattern:**
```sql
-- Two-step COALESCE: prodview_unit_id first, api_10 as fallback
LEFT JOIN well_360 w1 ON u.id_rec = w1.prodview_unit_id
LEFT JOIN well_360 w2 ON u.api_10 = w2.api_10
-- Then: COALESCE(w1.eid, w2.eid) AS eid
--       COALESCE(w1.eid, w2.eid) IS NULL AS is_eid_unresolved
```

**Decision:** Include ALL pvunitcomp units in the fact. Flag `is_eid_unresolved = true`
for the 18.5% without an EID. Do NOT drop them — production volumes appear in the fact
with NULL financial join columns, which correctly represents the data state. As FP wells
get EIDs assigned in ProdView, they will automatically start resolving on each rebuild.

**Follow-up task (not blocking this sprint):** Backfill `property_eid` on the ~1,145
FP Griffin well units in ProdView to bring EID resolution to ~97%+ (non-op and
injection wells are the legitimate remainder).

---

## Architecture Summary

**Medallion layers:**
- Gold (marts/) → normalized star schema, `eid` FK only
- Platinum (new: platinum/) → denormalized OBT, all well_360 identity embedded

**7 new models:**

| Model | Layer | Grain | Materialization |
|-------|-------|-------|----------------|
| `int_well_perf__prodview_monthly` | intermediate | EID + month | ephemeral |
| `int_well_perf__los_monthly` | intermediate | EID + month | ephemeral |
| `int_well_perf__drilling_summary` | intermediate | EID | ephemeral |
| `int_well_perf__completion_summary` | intermediate | EID | ephemeral |
| `fct_well_performance_monthly` | marts/well_360 | EID + month | table, cluster (eid, production_month) |
| `plat_well__performance_scorecard` | platinum/well_360 | EID | table |

**dbt_project.yml change required:** Add platinum layer config block under `formentera_analytics.operations:`.

---

## `/slfg` Prompt

Paste the following into a fresh session and run `/slfg`:

---

```
Build the Well Performance Mart for the Formentera Analytics dbt project.
This task introduces a new platinum/ layer alongside the existing marts/ layer.

## Medallion Architecture Context

This project uses:
- **Gold (marts/)** → normalized star schema, FK-based facts + dimensions, `FO_PRODUCTION_DB.marts`
- **Platinum (new: platinum/)** → denormalized OBT, pre-joined for BI consumption, `FO_PRODUCTION_DB.platinum`

Naming convention: `plat_{domain}__{description}`

---

## Step 0: Create the Platinum Layer (do this first)

### 1. Create the directory
```
mkdir -p models/operations/platinum/well_360/
```

### 2. Add config to dbt_project.yml

Add this block inside `formentera_analytics.operations:`,
immediately after the `marts:` block (around line 83):

```yaml
      # Platinum models - denormalized OBT, consumption-ready
      platinum:
        +database: "{{ {'prod': 'FO_PRODUCTION_DB', 'ci': 'FO_CI_DB', 'dev': target.database}.get(target.name, target.database) }}"
        +schema: "{{ target.schema if target.name in ['ci', 'dev'] else 'platinum' }}"
        +materialized: table
        +tags: ["platinum", "fo"]
```

### 3. Run `dbt parse` to confirm config is valid before writing any models

---

## What to Build

**7 new models total:**

**Intermediates (ephemeral, in `models/operations/intermediate/well_360/`):**
- `int_well_perf__prodview_monthly`
- `int_well_perf__los_monthly`
- `int_well_perf__drilling_summary`
- `int_well_perf__completion_summary`

**Gold mart (table, in `models/operations/marts/well_360/`):**
- `fct_well_performance_monthly`

**Platinum OBT (table, in `models/operations/platinum/well_360/`):**
- `plat_well__performance_scorecard`

Full architecture design is at `docs/brainstorms/2026-02-18-well-performance-mart-brainstorm.md`. Read it first.

---

## Context Files to Load Before Writing Code

1. `docs/brainstorms/2026-02-18-well-performance-mart-brainstorm.md` — architecture, grain decisions, non-goals
2. `docs/conventions/staging.md` — 5-CTE pattern (intermediates follow this)
3. `docs/conventions/intermediate.md` — materialization guidance
4. `docs/conventions/marts.md` — naming, YAML docs, required columns

---

## Models to Read Before Writing SQL

Read these to understand upstream column names BEFORE writing:

- `models/operations/staging/prodview/allocations/stg_prodview__daily_allocations.sql`
- `models/operations/staging/prodview/general/stg_prodview__units.sql` — has `api_10`, `id_rec`
- `models/operations/marts/finance/fct_los.sql` — LOS column names, signing logic
- `models/operations/marts/drilling/fct_daily_drilling_cost.sql`
- `models/operations/marts/drilling/fct_drilling_time.sql`
- `models/operations/marts/drilling/fct_npt_events.sql`
- `models/operations/marts/drilling/fct_stimulation.sql`
- `models/operations/marts/well_360/well_360.sql` — identity spine for platinum OBT

---

## Resolved Join Keys (do not re-derive)

| Source | EID Resolution |
|--------|----------------|
| ProdView | Two-step COALESCE: `id_rec_unit` → JOIN `stg_prodview__units` ON `id_rec`, then LEFT JOIN `well_360` twice: (1) `u.id_rec = w1.prodview_unit_id`, (2) `u.api_10 = w2.api_10`. Use `COALESCE(w1.eid, w2.eid)`. Filter to `unit_type = 'pvunitcomp'` first. 81.5% match rate. |
| fct_los | `right(well_code, 6)` — string derivation, no join. Filter `location_type = 'Well'` only. |
| fct_daily_drilling_cost | `eid` column already present |
| fct_drilling_time | `eid` column already present |
| fct_npt_events | `eid` column already present |
| fct_stimulation | `eid` column already present |

**NOTE:** ProdView EID join via api_10 was validated before this plan was written.
If match rate was <80%, an alternative join path was identified — check blocker notes
at the top of this document.

**DO NOT use `int_prodview__production_volumes`** — it is a legacy compatibility bridge
with quoted-alias outputs (`"Allocated Gas mcf"` etc.). Read directly from
`stg_prodview__daily_allocations` + join `stg_prodview__units`.

---

## Model Specs

### `int_well_perf__prodview_monthly` (ephemeral)
- Grain: `(id_rec_unit, production_month)` — use ProdView unit ID as spine, EID may be NULL
- Filter: `unit_type = 'pvunitcomp'` on `stg_prodview__units` to exclude facilities/externals
- Join path (two-step COALESCE):
  ```sql
  JOIN stg_prodview__units u ON a.id_rec_unit = u.id_rec
  LEFT JOIN well_360 w1 ON u.id_rec = w1.prodview_unit_id
  LEFT JOIN well_360 w2 ON u.api_10 = w2.api_10
  ```
- EID: `COALESCE(w1.eid, w2.eid) AS eid` — NULL for ~18.5% of wells
- Flag: `COALESCE(w1.eid, w2.eid) IS NULL AS is_eid_unresolved`
  -- NOTE: ~18.5% of pvunitcomp units will have is_eid_unresolved=true.
  -- The majority are FP Griffin wells migrated into ProdView without property_eid
  -- backfilled. They resolve automatically when EIDs are added to ProdView.
  -- A count of is_eid_unresolved rows is a useful FP onboarding progress metric.
- Aggregate daily rows to month using `date_trunc('month', allocation_date)::date`
- Key columns: `eid`, `id_rec_unit`, `production_month`, `is_eid_unresolved`,
  `oil_bbls`, `gas_mcf`, `water_bbls`, `ngl_bbls`, `gross_boe` (`oil_bbls + gas_mcf/6`)
- Use `coalesce(..., 0)` before BOE arithmetic to handle NULLs

### `int_well_perf__los_monthly` (ephemeral)
- Grain: `(eid, journal_month_start)`
- Source: `fct_los` — filter `location_type = 'Well'` only
- Derive EID: `right(well_code, 6) as eid`
- Pivot `los_category` into columns using `SUM(CASE WHEN los_category = 'Revenue' THEN los_gross_amount ELSE 0 END)`
- Key columns: `eid`, `journal_month_start` as `los_month`, `los_revenue`,
  `los_loe`, `los_severance_tax`, `los_net_income`
- Check actual `los_category` values with `dbt show --select fct_los --limit 20`
  before writing the CASE statements

### `int_well_perf__drilling_summary` (ephemeral)
- Grain: `eid`
- Sources: `fct_daily_drilling_cost`, `fct_drilling_time`, `fct_npt_events`
- All three carry `eid` — GROUP BY eid and SUM across all jobs
- Key columns: `eid`, `total_dc_cost`, `total_drilling_hours`, `total_npt_hours`,
  `npt_pct` (`total_npt_hours / nullif(total_drilling_hours, 0)`), `job_count`
  (count distinct job_id from drilling cost)

### `int_well_perf__completion_summary` (ephemeral)
- Grain: `eid`
- Source: `fct_stimulation` — already carries `eid`
- SUM across all stim jobs per well (multiple jobs per EID possible)
- Key columns: `eid`, `total_stages`, `total_proppant_lb`,
  `total_clean_volume_bbl`, `lateral_length_ft` (SUM of `length_gross_ft`),
  `proppant_per_ft_lb` (`total_proppant_lb / nullif(lateral_length_ft, 0)`)

### `fct_well_performance_monthly` (gold mart, table)
- Grain: `(eid, production_month)`
- Spine: `int_well_perf__prodview_monthly`
- LEFT JOIN `int_well_perf__los_monthly` ON `(eid, production_month = los_month)`
- Unique key: `well_performance_monthly_sk` = `generate_surrogate_key(['eid', 'production_month'])`
- Cluster by: `['eid', 'production_month']`
- **EID is the only well identity column** — no well_name, basin, etc.
  Join to well_360 at query time for those.
- Key columns: `well_performance_monthly_sk`, `eid`, `production_month`,
  all volume columns, `has_los_entry` boolean, all los_* financial columns
  (NULL when no LOS entry)
- Tags: `['marts', 'fo', 'well_360']`

### `plat_well__performance_scorecard` (platinum OBT, table)
- Grain: `eid` — one row per well, lifetime
- **Spine: `well_360`** — all known EIDs, even pre-production wells
- Aggregates rolled from `fct_well_performance_monthly` (build as a CTE joining the monthly fact)
- LEFT JOINs: `int_well_perf__drilling_summary`, `int_well_perf__completion_summary`
- **Denormalized** — embed all well_360 identity columns directly (no FK-only design):
  `well_name`, `api_10`, `api_14`, `cost_center_number`, `basin`, `state`, `county`,
  `operator_company_name`, `wellbore_status`, `spud_date`, `first_production_date`,
  `total_depth_ft`, and any other well_360 columns useful for filtering
- Production lifetime aggregates: `cumulative_oil_bbls`, `cumulative_gas_mcf`,
  `cumulative_water_bbls`, `cumulative_boe`, `peak_monthly_boe`, `peak_month`,
  `producing_months_count`, `first_production_month`, `latest_production_month`
- Financial lifetime aggregates: `cumulative_los_revenue`, `cumulative_los_loe`,
  `cumulative_los_net_income`, `months_with_los_count`
- D&C context: all columns from `int_well_perf__drilling_summary` +
  `has_drilling_data` boolean
- Completion context: all columns from `int_well_perf__completion_summary` +
  `has_completion_data` boolean
- Tags: `['platinum', 'fo', 'well_360']`

---

## YAML Documentation

Each new model needs a YAML entry. Check existing files in:
- `models/operations/intermediate/well_360/` for `_int_well_360.yml` naming pattern
- `models/operations/marts/well_360/` for mart YAML pattern
- Create `models/operations/platinum/well_360/_plat_well_360.yml`

Minimum per model: `description`, `columns` with `description` on every output column.

---

## Critical Patterns and Gotchas

**dbt YAML test syntax (1.11+) — always use config: wrapper:**
```yaml
data_tests:
  - not_null:
      config:
        severity: warn
```
Bare `severity: warn` without `config:` wrapper passes partial parse but fails CI `--warn-error`.

**Fan-out prevention:** GROUP BY on natural key only in aggregation models.
Never add flag columns to GROUP BY. Use `SUM(CASE WHEN ...)` for conditional splits.

**accepted_values needs arguments: wrapper:**
```yaml
- accepted_values:
    arguments:
      values: ['A', 'B']
```

**BOE formula:** `coalesce(oil_bbls, 0) + (coalesce(gas_mcf, 0) / 6)`

**LOS signing:** `fct_los.los_gross_amount` / `los_net_amount` are pre-signed
(costs already flipped negative). Use these, not raw `gross_amount`.

**Surrogate key for monthly fact:**
`{{ dbt_utils.generate_surrogate_key(['eid', 'production_month']) }}`

**No surrogate key on platinum OBT** — EID is the natural PK. No SK needed.

**Validate after each intermediate, not just at the end.** Run
`dbt show --select <model> --limit 10` after each intermediate to confirm
EID resolution is working before building the marts.

---

## Acceptance Criteria

1. `dbt parse --warn-error --no-partial-parse` passes — YAML is clean
2. `dbt build --select int_well_perf__prodview_monthly+` runs clean
3. `dbt build --select int_well_perf__los_monthly+` runs clean
4. `dbt build --select fct_well_performance_monthly` — confirm row count
5. `dbt build --select plat_well__performance_scorecard` — confirm row count ~9K-15K
6. Spot check: `dbt show` a known EID across both output models and confirm
   production volumes + financial data + D&C data all join correctly

---

## Validation Queries

```sql
-- BOE sanity: top producing wells
SELECT eid, production_month, oil_bbls, gas_mcf, gross_boe
FROM fct_well_performance_monthly
ORDER BY gross_boe DESC LIMIT 10

-- LOS join rate
SELECT
  COUNT(*) AS total_well_months,
  ROUND(100.0 * SUM(CASE WHEN has_los_entry THEN 1 ELSE 0 END) / COUNT(*), 1) AS los_pct
FROM fct_well_performance_monthly

-- Scorecard completeness
SELECT
  COUNT(*) AS total_wells,
  SUM(CASE WHEN has_drilling_data THEN 1 ELSE 0 END) AS with_drilling,
  SUM(CASE WHEN has_completion_data THEN 1 ELSE 0 END) AS with_completion,
  SUM(CASE WHEN cumulative_boe > 0 THEN 1 ELSE 0 END) AS with_production
FROM plat_well__performance_scorecard
```

---

## Branch

Create branch `feature/well-performance-mart` off `main`.
```
