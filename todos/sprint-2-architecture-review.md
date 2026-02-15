# Sprint 2 WellView Staging Refactor -- Architecture Review

**Reviewer:** System Architecture Review (Claude Opus 4.6)
**Date:** 2026-02-13
**Branch:** `feature/sprint-1-drilling-dimensions`
**Scope:** 47 files changed, 4,094 insertions, 2,058 deletions

---

## 1. Architecture Overview

This sprint refactors 10 WellView staging models from a legacy 2-CTE pattern
(`source -> renamed`) to the project-standard 5-CTE pipeline
(`source -> renamed -> filtered -> enhanced -> final`). It also reorganizes 26
models from a flat directory into 3 domain subdirectories (`general/`,
`wellbore_surveys/`, `operations/`), introduces per-domain split-source YAML
files, and cascades a quoted-to-snake_case naming convention change through 7
intermediate models and 2 mart models.

The refactor follows the established ProdView pattern that was already proven in
this codebase and is well-documented in `CLAUDE.md`.

---

## 2. Change Assessment -- What Aligns Well

### 2.1 Domain Directory Organization: PASS

The 3-domain directory structure under `models/operations/staging/wellview/`
mirrors the ProdView pattern exactly. Each domain has:
- `_src_wellview.yml` -- source declarations
- `_stg_wellview.yml` -- model documentation and tests
- SQL model files

This is architecturally sound and consistent with the existing codebase pattern.

### 2.2 Split-Source YAML Pattern: PASS

All three `_src_wellview.yml` files declare the same source name
(`wellview_calcs`), database (`PELOTON_FORMENTERAOPS_FORMENTERAOPS_WV120`), and
schema (`FORMENTERAOPS_WV120_CALC`). This matches the ProdView split-source
pattern where dbt merges tables from files sharing the same source name.

The root `src_wellview_calcs.yml` has been properly updated to remove migrated
tables and includes a comment header explaining where domain-specific tables
now live.

### 2.3 5-CTE Pattern Adherence: PASS (all 10 models)

All 10 refactored staging models follow the standard pipeline:

| CTE | Purpose | Verified |
|-----|---------|----------|
| `source` | Raw pull + Fivetran dedup via `qualify row_number()` | Yes -- all 10 |
| `renamed` | Rename, cast, trim. No filtering, no logic. | Yes -- all 10 |
| `filtered` | `coalesce(_fivetran_deleted, false) = false` + PK not null | Yes -- all 10 |
| `enhanced` | Surrogate key + `_loaded_at` | Yes -- all 10 |
| `final` | Explicit column list, logically grouped | Yes -- all 10 |

### 2.4 Config Tags: PASS

Every model uses appropriate 3-tag config:
`tags=['wellview', 'staging', '{domain}']` where domain is `general`,
`wellbore_surveys`, or `operations`.

### 2.5 Downstream Cascade -- Naming Convention: PASS

The quoted-to-snake_case cascade has been correctly applied:

- **Staging models** output snake_case (clean internal contracts)
- **Intermediate models** consume snake_case, output snake_case
- **Mart models** consume snake_case, alias back to quoted names for dashboards

Specific cascade verification:

| Model | Consumes snake_case | Outputs |
|-------|-------------------|---------|
| `int_well__wellview` | `eid`, `well_name`, `api_10_number` | snake_case |
| `int_well__spine` | `eid`, `api_10_number` | snake_case |
| `int_wellview__well_header` | `well_id`, `api_10_number`, `cost_center`, etc. | snake_case |
| `int_wellview__canonical_wells` | `stg_wellview__well_header` fields | snake_case |
| `int_wellview_job` | `job_id`, `well_id`, `cost_center` | snake_case |
| `int_fct_well_header` | `well_id`, `spud_date`, etc. from WV intermediate | Quoted (preserves existing downstream contract) |
| `fct_eng_jobs` | snake_case from `int_wellview_job` | Quoted via aliasing (e.g., `job_id as "Job ID"`) |
| `dim_job` | snake_case from `stg_wellview__jobs` | snake_case (new Sprint 1 mart) |

### 2.6 Macro Usage and Unit Conversions: MOSTLY PASS

The `wv_*` macro library (148 lines, 16 macros) correctly encapsulates all
Peloton-sourced conversion factors. The new `wv_per_m_to_per_100ft` macro was
added as planned.

### 2.7 Schema YAML Documentation: PASS

The `_stg_wellview.yml` files provide column-level documentation with source
field mapping (e.g., `"Well name. WellView: WELLNAME"`). Primary keys have
`unique` + `not_null` tests. Foreign keys have `not_null` with `severity: warn`.
This follows the project testing patterns.

### 2.8 Old Files Properly Removed: PASS

The original root-level SQL files for all 10 refactored models have been deleted.
No duplicate model definitions exist. The `grep` for `stg_wellview__well_header`
and `stg_wellview__jobs` in `*.sql` under the wellview root returns no matches.

---

## 3. Findings -- Issues to Address

### FINDING-01: Inline magic numbers remain in refactored models [MEDIUM]

**Files:**
- `/Users/robstover/Development/formentera/analytics/analytics/models/operations/staging/wellview/operations/stg_wellview__jobs.sql` (lines 131-143)
- `/Users/robstover/Development/formentera/analytics/analytics/models/operations/staging/wellview/operations/stg_wellview__rigs.sql` (lines 73-81)
- `/Users/robstover/Development/formentera/analytics/analytics/models/operations/staging/wellview/operations/stg_wellview__job_program_phases.sql` (line 110)
- `/Users/robstover/Development/formentera/analytics/analytics/models/operations/staging/wellview/operations/stg_wellview__job_reports.sql` (line 248)

**Description:**
The plan explicitly states "Replace ALL inline magic numbers with `wv_*` macros."
However, several refactored models still contain raw divisor constants:

1. **stg_wellview__jobs.sql**: 12 occurrences of `/ 3.28083989501312` for
   cost-per-foot conversions. This is the reciprocal of the meters-to-feet factor
   and should use a macro. The plan mentions: "Use `wv_meters_to_feet` for
   cost/ft" but the factor `3.28083989501312` is `1/0.3048` and semantically
   represents a "per-meter to per-foot" conversion, not a "meters to feet"
   conversion. The existing `wv_meters_to_feet` macro divides by `0.3048` which
   would produce the wrong result for cost/m to cost/ft.

   Additionally: `/ 24` for cost-per-day to cost-per-hour (4 occurrences).

2. **stg_wellview__rigs.sql**: `/ 4448.2216152605` (kN to klbf, 4 occurrences),
   `/ 1.3558179483314` (N-m to ft-lbf, 1 occurrence). These unit conversions
   have no corresponding macro.

3. **stg_wellview__job_program_phases.sql**: `/ 0.00227836103820356`
   (instantaneous ROP conversion). No macro exists for this conversion.

4. **stg_wellview__job_reports.sql**: `/ 0.555555555555556 + 32` (Celsius to
   Fahrenheit). No macro exists for temperature conversion.

**Recommendation:**
Add these missing macros to `wellview_unit_conversions.sql`:
- `wv_per_m_to_per_ft(column_name)` -- for cost/meter to cost/foot conversions
  (divisor: `3.28083989501312`)
- `wv_kn_to_klbf(column_name)` -- kilonewtons to kilo-pound-force
  (divisor: `4448.2216152605`)
- `wv_nm_to_ft_lbf(column_name)` -- newton-meters to foot-pound-force
  (divisor: `1.3558179483314`)
- `wv_celsius_to_fahrenheit(column_name)` -- temperature conversion
- `wv_days_to_hours` already handles `/ 24` equivalently (factor =
  `0.0416666666666667` = `1/24`), but some places use literal `/ 24` instead
  of the macro. Replace those with the macro.

For the `/ 24` instances specifically in `stg_wellview__jobs.sql` lines 126-129
and `stg_wellview__job_reports.sql` lines 134-135: these should use
`wv_days_to_hours` or a dedicated `wv_per_day_to_per_hour` macro.

**Priority:** Medium. The conversions are correct, but they violate the stated
goal of eliminating all inline magic numbers. This is a consistency issue, not
a correctness issue.

---

### FINDING-02: Audit timestamp naming inconsistency across models [LOW]

**Files:** All 10 refactored staging models

**Description:**
The audit timestamp columns use inconsistent naming across the 10 refactored
staging models:

| Model | Created | Modified |
|-------|---------|----------|
| `stg_wellview__well_header` | `created_at_utc` | `last_mod_at_utc` |
| `stg_wellview__jobs` | `created_at` | `last_mod_at` |
| `stg_wellview__wellbores` | `created_at_utc` | `modified_at_utc` |
| `stg_wellview__job_reports` | `created_at` | `modified_at` |
| `stg_wellview__job_time_log` | `created_at` | `modified_at` |
| `stg_wellview__rigs` | `created_at` | `modified_at` |
| `stg_wellview__job_program_phases` | `created_at_utc` | `modified_at_utc` |
| `stg_wellview__daily_costs` | `created_at` | `modified_at` |
| `stg_wellview__job_afe_definitions` | `created_at_utc` | `modified_at_utc` |
| `stg_wellview__job_interval_problems` | `created_at` | `modified_at` |

Three patterns exist:
- `created_at_utc` / `last_mod_at_utc` (well_header only)
- `created_at_utc` / `modified_at_utc` (wellbores, phases, afe_definitions)
- `created_at` / `modified_at` (jobs, reports, time_log, rigs, costs, problems)

**Recommendation:**
Pick one convention and apply it consistently. The `_utc` suffix is more explicit
since the source is `timestamp_ntz` (which could be ambiguous about timezone).
Suggest standardizing on `created_at_utc` / `modified_at_utc` across all WellView
staging models. The well_header model's `last_mod_at_utc` is uniquely abbreviated
compared to the `modified_at_utc` pattern used elsewhere.

**Priority:** Low. This is a cosmetic inconsistency. It does not affect
correctness but creates friction when building downstream models that join
multiple staging tables.

---

### FINDING-03: int_wellview_job uses `select *` for jobs CTE [LOW]

**File:** `/Users/robstover/Development/formentera/analytics/analytics/models/operations/intermediate/production/int_wellview_job.sql`

**Description:**
The `int_wellview_job` intermediate model uses `select * from
{{ ref('stg_wellview__jobs') }}` in the `jobs` CTE (line 9). While the final
SELECT is explicit, the `select *` means any column additions to the staging
model will flow through automatically, which contradicts the explicit-column-list
principle stated in `CLAUDE.md` for intermediate and mart layers.

The `wells` CTE also uses `select * from {{ ref('stg_wellview__well_header') }}`
(line 14).

**Recommendation:**
Replace the `select *` CTEs with explicit column lists of only the columns
actually used in the final SELECT. This matches the pattern used in
`int_well__wellview` which explicitly selects only needed columns.

**Priority:** Low. The risk is minimal since the final SELECT is explicit, but
it is a deviation from the project convention.

---

### FINDING-04: int_wellview_job overrides default materialization [INFO]

**File:** `/Users/robstover/Development/formentera/analytics/analytics/models/operations/intermediate/production/int_wellview_job.sql`

**Description:**
This model sets `materialized='view'` explicitly. Per `CLAUDE.md`, the default
materialization for intermediate models is `ephemeral` and models should not
override this "unless you have a specific reason." The model is consumed by
exactly 2 downstream models (`fct_eng_jobs` and `dim_job`), which is below the
threshold of 3+ consumers that would justify a `table` override.

This is likely a pre-existing condition (the model already had this config before
the refactor), so it is not introduced by this change.

**Recommendation:**
No action required for this PR. Consider removing the `materialized='view'`
override in a future cleanup pass to let the ephemeral default apply.

**Priority:** Info only.

---

### FINDING-05: int_wellview__canonical_wells is an orphan model [INFO]

**File:** `/Users/robstover/Development/formentera/analytics/analytics/models/operations/intermediate/operations/int_wellview__canonical_wells.sql`

**Description:**
The plan explicitly notes this model has "no consumers." It was updated to use
snake_case column names from the refactored staging model, which is correct to
maintain the cascade. However, a model with zero downstream consumers is dead
code that adds to parse time and cognitive overhead.

**Recommendation:**
Consider disabling this model (`enabled=false` in config) or removing it if it
has no planned use in upcoming sprints. At minimum, add a comment noting it is
unused pending future use.

**Priority:** Info only.

---

### FINDING-06: int_fct_well_header preserves quoted output names correctly [PASS]

**File:** `/Users/robstover/Development/formentera/analytics/analytics/models/operations/intermediate/production/int_fct_well_header.sql`

**Description:**
This model was correctly updated to consume snake_case from
`int_wellview__well_header` while preserving its quoted output column names
(e.g., `w.well_id` as `"Well ID"`, `w.spud_date` as `"Spud Date"`). This
protects the `fct_eng_well_header` mart and its downstream dashboards from
breaking changes.

The deeply nested `case` logic for company name normalization is pre-existing
technical debt, not introduced by this PR. No action needed here.

---

### FINDING-07: cost-per-foot conversion CONFIRMED BUG in job_reports and job_program_phases [HIGH]

**File:** `/Users/robstover/Development/formentera/analytics/analytics/models/operations/staging/wellview/operations/stg_wellview__jobs.sql` (lines 131-143)

**Description:**
The cost-per-foot columns use `/ 3.28083989501312` to convert cost-per-meter
to cost-per-foot. Meanwhile, `stg_wellview__job_reports.sql` uses
`{{ wv_meters_to_feet('costperdepthcalc') }}` (which divides by `0.3048`) for
what appears to be the same type of column (`cost_per_depth_per_ft`).

These produce different results:
- `/ 3.28083989501312` = multiply by `0.3048` (correct for cost/m to cost/ft)
- `/ 0.3048` = multiply by `3.28083989501312` (correct for meters to feet, but
  WRONG for cost/m to cost/ft)

If a column stores "dollars per meter," converting to "dollars per foot" requires
multiplying by `0.3048` (fewer feet = lower rate per foot), which is what
`/ 3.28083989501312` does.

But the job_reports model uses `wv_meters_to_feet` for cost-per-depth columns,
which divides by `0.3048` and would produce the INVERSE of the correct answer.

**Confirmed via Peloton source script:** The raw conversion script
(`wellview_unit_conversions_raw.txt`) uses `/ 3.28083989501312` for ALL
cost-per-depth columns. This confirms:
- `stg_wellview__jobs.sql` is CORRECT (inline `/ 3.28083989501312`)
- `stg_wellview__job_reports.sql` is WRONG (8 columns use `wv_meters_to_feet`)
- `stg_wellview__job_program_phases.sql` is WRONG (6 columns use `wv_meters_to_feet`)

**Recommendation:**
1. Add a `wv_per_m_to_per_ft` macro (divisor: `3.28083989501312`)
2. Replace `wv_meters_to_feet` with `wv_per_m_to_per_ft` on all 14 affected columns
3. Replace the 12 inline constants in jobs.sql with the new macro

See `todos/FINDING-07-cost-per-depth-conversion.md` for the complete list of
affected columns.

**Priority:** HIGH -- BLOCKING. This is a confirmed data correctness issue.
The 14 affected columns in job_reports and job_program_phases will produce
values ~10.76x too large.

---

### FINDING-08: Missing `_stg_wellview.yml` entries for non-refactored models [LOW]

**Description:**
The `_stg_wellview.yml` in the `operations/` domain includes entries for all 10
refactored models plus placeholder entries for the un-refactored models that were
moved (verified via the file). The `general/` domain includes entries for
`stg_wellview__reference_wells` and `stg_wellview__well_status_history` that note
they are "Not yet refactored to 5-CTE pattern." This is good documentation.

However, the wellbore_surveys domain YAML only documents `stg_wellview__wellbores`
(the refactored model). The 3 other models in that directory
(`stg_wellview__wellbore_depths`, `stg_wellview__wellbore_directional_surveys`,
`stg_wellview__wellbore_directional_survey_data`) should have at least minimal
entries with `unique` + `not_null` tests on their primary keys.

**Recommendation:**
Add skeleton entries for the 3 un-refactored wellbore survey models in
`wellbore_surveys/_stg_wellview.yml` with PK tests, matching the pattern used
in `general/_stg_wellview.yml`.

**Priority:** Low.

---

### FINDING-09: dbt_project.yml drilling mart schema routing [PASS]

**File:** `/Users/robstover/Development/formentera/analytics/analytics/dbt_project.yml`

**Description:**
The new `drilling` subdirectory config correctly follows the existing pattern:
- Dev/CI: routes to `target.schema` (user's dev schema or CI schema)
- Prod: routes to dedicated `drilling` schema

Tags are set to `['drilling', 'mart']`. Materialization is `table`, consistent
with all other mart directories.

---

## 4. Risk Analysis

### Low Risk
- **Schema breakage:** The cascade is complete. All downstream consumers of the
  2 renamed staging models have been updated. Models out of scope
  (`fct_eng_well_header`, ProdView intermediates) consume `int_fct_well_header`
  which preserves its existing output contract.
- **Split-source collision:** All 4 source YAML files (3 domain + 1 root) use
  identical `name`, `database`, and `schema` values, so dbt will merge them
  correctly.

### Medium Risk
- **FINDING-07 (cost conversion math):** If the `job_reports` model is using
  `wv_meters_to_feet` for cost-per-depth columns when it should be using a
  cost/m-to-cost/ft conversion, those values will be off by a factor of ~10.76x.
  This needs investigation before merging to production.

### Minimal Risk
- **Magic numbers (FINDING-01):** Functionally correct, but violates the stated
  refactor goals. Can be cleaned up in a follow-up.
- **Naming inconsistency (FINDING-02):** Cosmetic, no downstream impact.

---

## 5. Recommendations Summary

| ID | Severity | Action | Blocking? |
|----|----------|--------|-----------|
| FINDING-01 | MEDIUM | Add missing macros, replace remaining inline constants | No |
| FINDING-02 | LOW | Standardize audit timestamp naming | No |
| FINDING-03 | LOW | Replace `select *` in int_wellview_job CTEs | No |
| FINDING-04 | INFO | Future: remove `materialized='view'` override | No |
| FINDING-05 | INFO | Consider disabling orphan model | No |
| FINDING-06 | PASS | No action | -- |
| FINDING-07 | HIGH | Fix 14 wrong cost-per-depth conversions in job_reports + phases | YES |
| FINDING-08 | LOW | Add PK test entries for 3 un-refactored wellbore models | No |
| FINDING-09 | PASS | No action | -- |

---

## 6. Overall Assessment

**Verdict: APPROVE with one blocking investigation (FINDING-07)**

The refactor is architecturally sound, follows established patterns, and
maintains system integrity through a well-planned cascade. The split-source
YAML pattern, 5-CTE adherence, and downstream cascade are all correctly
implemented. The domain directory organization mirrors ProdView and sets up
a clean pattern for Sprint 3+ domains.

The one blocking concern is the potential cost-per-depth conversion discrepancy
between `stg_wellview__jobs` (which uses an inline `/ 3.28083989501312`) and
`stg_wellview__job_reports` (which uses `wv_meters_to_feet` macro for the same
semantic conversion). These produce mathematically opposite results for
cost-rate columns. This must be investigated and resolved before production
deployment.
