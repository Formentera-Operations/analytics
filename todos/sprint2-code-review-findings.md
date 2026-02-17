# Sprint 2 WellView Staging Refactor -- Code Pattern Review

Date: 2026-02-13
Reviewer: Claude Code (pattern analysis)
Branch: feature/sprint-1-drilling-dimensions

## Summary

Reviewed 10 refactored staging models, 4 drilling marts, 6 YAML files, and 1 macro file.
Overall quality is high. The 5-CTE pattern is consistently applied across all 10 models.
Findings below are grouped by severity.

---

## CRITICAL (0 findings)

None.

---

## HIGH -- Inconsistencies That Will Cause Confusion (3 findings)

### H1. Audit Column Naming Inconsistency Across Refactored Models

The 10 refactored models use THREE different naming conventions for the same audit columns.
This will cause join/union issues in downstream models and confuse analysts.

| Model | Created | Modified |
|-------|---------|----------|
| `stg_wellview__well_header` | `created_at_utc` / `last_mod_at_utc` / `last_mod_by` | (uses `last_mod_*` prefix) |
| `stg_wellview__jobs` | `created_at` / `last_mod_at` / `last_mod_by` | (uses `last_mod_*` prefix, NO `_utc` suffix) |
| `stg_wellview__rigs` | `created_at` / `modified_at` / `modified_by` | (uses `modified_*` prefix) |
| `stg_wellview__job_reports` | `created_at` / `modified_at` / `modified_by` | (uses `modified_*` prefix) |
| `stg_wellview__job_interval_problems` | `created_at` / `modified_at` / `modified_by` | (uses `modified_*` prefix) |
| `stg_wellview__job_time_log` | `created_at` / `modified_at` / `modified_by` | (uses `modified_*` prefix) |
| `stg_wellview__daily_costs` | `created_at` / `modified_at` / `modified_by` | (uses `modified_*` prefix) |
| `stg_wellview__wellbores` | `created_at_utc` / `modified_at_utc` / `modified_by` | (has `_utc`, uses `modified_*`) |
| `stg_wellview__job_program_phases` | `created_at_utc` / `modified_at_utc` / `modified_by` | (has `_utc`, uses `modified_*`) |
| `stg_wellview__job_afe_definitions` | `created_at_utc` / `modified_at_utc` / `modified_by` | (has `_utc`, uses `modified_*`) |

Three variants:
  A. `created_at_utc` + `last_mod_at_utc` + `last_mod_by` (well_header pattern)
  B. `created_at` + `last_mod_at` + `last_mod_by` (jobs pattern -- no _utc suffix)
  C. `created_at` + `modified_at` + `modified_by` (majority pattern)
  D. `created_at_utc` + `modified_at_utc` + `modified_by` (wellbores, phases, afe_defs)

Recommendation: Pick ONE convention and apply it everywhere. The CLAUDE.md example uses
`created_at_utc` and `created_by`, so the `_utc` suffix + `last_mod_*` pattern is the
documented standard. However, `modified_at` / `modified_by` is the most common across
the 10 files. Decide which is canonical and refactor.

Files:
- /Users/robstover/Development/formentera/analytics/analytics/models/operations/staging/wellview/general/stg_wellview__well_header.sql (lines 196-199)
- /Users/robstover/Development/formentera/analytics/analytics/models/operations/staging/wellview/operations/stg_wellview__jobs.sql (lines 267-270)
- /Users/robstover/Development/formentera/analytics/analytics/models/operations/staging/wellview/operations/stg_wellview__rigs.sql (lines 97-100)
- /Users/robstover/Development/formentera/analytics/analytics/models/operations/staging/wellview/operations/stg_wellview__job_reports.sql (lines 322-326)
- /Users/robstover/Development/formentera/analytics/analytics/models/operations/staging/wellview/wellbore_surveys/stg_wellview__wellbores.sql (lines 142-146)
- /Users/robstover/Development/formentera/analytics/analytics/models/operations/staging/wellview/operations/stg_wellview__job_program_phases.sql (lines 276-280)
- /Users/robstover/Development/formentera/analytics/analytics/models/operations/staging/wellview/operations/stg_wellview__job_afe_definitions.sql (lines 104-108)


### H2. System Lock Fields: Inconsistent Data Types

Two models cast system lock fields (`syslockme`, `syslockchildren`, `syslockmeui`,
`syslockchildrenui`) as `varchar`, while the remaining eight cast them as `boolean`.

Models with varchar typing:
- `stg_wellview__wellbores` (lines 135-139)
- `stg_wellview__job_program_phases` (lines 269-273)

All other refactored models use `::boolean`. These two should be aligned to boolean.

Files:
- /Users/robstover/Development/formentera/analytics/analytics/models/operations/staging/wellview/wellbore_surveys/stg_wellview__wellbores.sql
- /Users/robstover/Development/formentera/analytics/analytics/models/operations/staging/wellview/operations/stg_wellview__job_program_phases.sql


### H3. System Lock Fields Split Into Separate Comment Group in Some Models

In `stg_wellview__wellbores` and `stg_wellview__job_program_phases`, the system lock
fields are separated into their own `-- system locking` comment group in the final CTE,
rather than being included under `-- system / audit` like the other 8 models. This
creates a gratuitous structural difference.

Files:
- /Users/robstover/Development/formentera/analytics/analytics/models/operations/staging/wellview/wellbore_surveys/stg_wellview__wellbores.sql (line 289)
- /Users/robstover/Development/formentera/analytics/analytics/models/operations/staging/wellview/operations/stg_wellview__job_program_phases.sql (line 559)

---

## MEDIUM -- Inline Magic Numbers Not Yet Macro-ized (5 findings)

### M1. Inline Cost-Per-Foot Conversion Factor (/ 3.28083989501312)

`stg_wellview__jobs` has 12 lines using the raw divisor `/ 3.28083989501312` for
cost-per-meter to cost-per-foot conversion. There is no `wv_cost_per_m_to_per_ft()`
macro, but the factor is the reciprocal of `wv_meters_to_feet`'s 0.3048, so the
existing `wv_meters_to_feet` macro could be used. These are "inverse" conversions
($/m to $/ft), not length conversions, so a dedicated macro like
`wv_per_m_to_per_ft()` would be clearest.

Note: `stg_wellview__job_reports` and `stg_wellview__job_program_phases` also use this
factor via `wv_meters_to_feet` for cost_per_depth columns. The inconsistency is that
`stg_wellview__jobs` uses the raw number while sibling models use the macro.

File: /Users/robstover/Development/formentera/analytics/analytics/models/operations/staging/wellview/operations/stg_wellview__jobs.sql (lines 132-143)


### M2. Inline Days-To-Hours (/ 24) for Cost-Per-Hour

`stg_wellview__jobs` (lines 126-129) and `stg_wellview__job_reports` (lines 134-135)
use raw `/ 24` for cost-per-day to cost-per-hour. The project has `wv_days_to_hours()`
but it uses Peloton's divisor `0.0416666666666667` which is 1/24. Semantically these
ARE the same conversion, but the inline `/ 24` looks different. Consider using the
macro for visual consistency, or adding a comment explaining the equivalence.

Files:
- /Users/robstover/Development/formentera/analytics/analytics/models/operations/staging/wellview/operations/stg_wellview__jobs.sql (lines 126-129)
- /Users/robstover/Development/formentera/analytics/analytics/models/operations/staging/wellview/operations/stg_wellview__job_reports.sql (lines 134-135)


### M3. Inline Proportion-To-Percent (/ 0.01) -- No Macro Exists

Multiple refactored models convert WellView proportions (0.0-1.0) to percentages
using `/ 0.01`. This pattern appears in:
- `stg_wellview__jobs` (7 occurrences, lines 206-212)
- `stg_wellview__job_reports` (20 occurrences, lines 199-230)
- `stg_wellview__job_program_phases` (5 occurrences, lines 154-158)
- `stg_wellview__job_afe_definitions` (1 occurrence, line 111)
- `stg_wellview__daily_costs` (1 occurrence, line 69)

A `wv_proportion_to_pct()` macro would make intent clearer and match the pattern of
the other `wv_*` macros. Lower priority since `/ 0.01` is well-commented in context.


### M4. Inline Force Conversion (/ 4448.2216152605) in Rigs

`stg_wellview__rigs` uses raw `/ 4448.2216152605` for kilonewtons-to-klbf conversions
(lines 73-78). The macro file has `wv_newtons_to_lbf()` which divides by 4.4482216152605
(newtons, not kilonewtons). The rig model divides by 1000x that factor because the source
stores values in newtons but the output is in klbf (thousands). A `wv_newtons_to_klbf()`
or `wv_kn_to_klbf()` macro would clarify this.

File: /Users/robstover/Development/formentera/analytics/analytics/models/operations/staging/wellview/operations/stg_wellview__rigs.sql (lines 73-78)


### M5. Inline Torque Conversion (/ 1.3558179483314) in Rigs

`stg_wellview__rigs` has a raw torque conversion (N-m to ft-lbf) on line 81. No macro
exists for this conversion. A `wv_nm_to_ft_lbf()` macro would follow the established
pattern.

File: /Users/robstover/Development/formentera/analytics/analytics/models/operations/staging/wellview/operations/stg_wellview__rigs.sql (line 81)

---

## LOW -- Style/Convention Nitpicks (6 findings)

### L1. CTE Comment Numbering: Only 2 of 10 Models Have Full CTE Comments

The CLAUDE.md pattern shows numbered CTE comments like:
  `-- 1. SOURCE: ...`
  `-- 2. RENAMED: ...`
  `-- 3. FILTERED: ...`
  `-- 4. ENHANCED: ...`
  `-- 5. FINAL: ...`

Only `stg_wellview__well_header` and `stg_wellview__jobs` have the full numbered comments.
The other 8 models have the CTE names but lack the numbered prefix comments on each CTE.
This is cosmetic but reduces readability for new contributors.

Models missing CTE number comments:
- stg_wellview__rigs
- stg_wellview__job_reports
- stg_wellview__job_interval_problems
- stg_wellview__job_time_log
- stg_wellview__daily_costs
- stg_wellview__wellbores
- stg_wellview__job_program_phases
- stg_wellview__job_afe_definitions


### L2. Renamed CTE Description Inconsistency

`stg_wellview__well_header` says: "Column renaming, type casting, trimming. No filtering, no logic."
`stg_wellview__jobs` says: "Column renaming, type casting, unit conversions. No filtering, no logic."
The jobs version is more accurate since it includes unit conversions in the renamed CTE.


### L3. Percentage Column Naming Inconsistency

`stg_wellview__jobs` uses the `_pct` suffix (e.g., `problem_time_pct`, `rotating_time_pct`).
`stg_wellview__job_reports` uses the `_percentage` and `percent_*` prefix (e.g.,
`problem_time_percentage`, `percent_depth_rotating`).
`stg_wellview__job_program_phases` uses the `percent_*` prefix (e.g., `percent_problem_time`).

Pick one convention. `_pct` suffix is more concise and aligns with oil & gas conventions.


### L4. Tag Inconsistency: 'general' vs 'operations' vs 'wellbore_surveys'

The third tag in the config block varies by subdirectory:
- `stg_wellview__well_header`: `tags=['wellview', 'staging', 'general']`
- Operations models: `tags=['wellview', 'staging', 'operations']`
- `stg_wellview__wellbores`: `tags=['wellview', 'staging', 'wellbore_surveys']`

This is intentional (matching subdirectory) but worth confirming it is desired. The CLAUDE.md
example uses the source name as the first tag (e.g., `tags=['source_name', 'staging', 'formentera']`).
The WellView models use `'wellview'` + `'staging'` + subdirectory, which deviates from the
documented `source_name` + `staging` + `formentera` pattern. Not necessarily wrong -- just
different from the CLAUDE.md example.


### L5. Primary Key Column Naming: `record_id` vs Domain-Specific Name

Three models use the generic `record_id` as the primary key column name:
- `stg_wellview__wellbores` (record_id)
- `stg_wellview__job_program_phases` (record_id)
- `stg_wellview__job_interval_problems` (record_id)

While the other 7 use domain-specific names:
- `well_id`, `job_id`, `job_rig_id`, `report_id`, `time_log_id`, `cost_line_id`, `job_afe_id`

The generic `record_id` loses context when referenced in downstream models. Consider
renaming to `wellbore_id`, `phase_id`, `interval_problem_id` respectively.

Note: In `dim_wellbore.sql` and `dim_phase.sql`, the marts already alias these:
`wb.record_id as wellbore_id` and `p.record_id as phase_id`. This confirms the staging
models should have used the specific names to begin with.


### L6. `stg_wellview__jobs` User Boolean Cast Uses CASE Instead of ::boolean

Line 263-264: `case when userboolean1 = 1 then true else false end as user_boolean_1`
while `stg_wellview__job_reports` (line 318-319) uses `userboolean1::boolean as user_boolean_1`.
The CASE pattern suggests the source column might be numeric (int) rather than boolean,
but if so, `userboolean1::boolean` should still work in Snowflake (0=false, non-0=true).
Verify source type and align the pattern.

File: /Users/robstover/Development/formentera/analytics/analytics/models/operations/staging/wellview/operations/stg_wellview__jobs.sql (lines 263-264)

---

## POSITIVE PATTERNS (Well-Implemented)

### P1. 5-CTE Pattern Universally Applied
All 10 refactored models follow the source -> renamed -> filtered -> enhanced -> final
pipeline. The CTE separation of concerns is clean.

### P2. Fivetran Dedup Pattern Consistent
All 10 models use `qualify 1 = row_number() over (partition by <pk> order by _fivetran_synced desc)`
in the source CTE. The partition key matches the appropriate primary key for each table.

### P3. Soft Delete Filter Consistent
All 10 models use `where coalesce(_fivetran_deleted, false) = false and <pk> is not null`
in the filtered CTE.

### P4. Surrogate Key Pattern Consistent
All 10 models generate surrogate keys in the enhanced CTE using
`dbt_utils.generate_surrogate_key(['<pk>'])`. Key names follow `{entity}_sk` convention.

### P5. _loaded_at Added Consistently
All 10 models include `current_timestamp() as _loaded_at` in the enhanced CTE.

### P6. Explicit Column List in Final CTE
All 10 models enumerate every column in the final CTE with logical grouping comments.
No `select *` in the output contract.

### P7. wv_* Macro Usage for Standard Conversions
Length, volume, rate, density, speed, and time conversions use the `wv_*` macro library
consistently. Only specialized conversions (force, torque, proportion, temperature,
cost-per-unit) remain inline.

### P8. Macro Library is Well-Documented
`wellview_unit_conversions.sql` includes source provenance (Peloton conversion script),
category grouping, and inline Peloton factor references for each macro.

### P9. Drilling Marts Are Clean
`dim_job`, `dim_wellbore`, `dim_phase`, `bridge_job_afe` are well-structured with:
- Clear CTE naming (source, joined)
- EID resolution via well_360
- Appropriate column selection (not carrying 200+ staging columns)
- Proper surrogate key generation
- Good test coverage in schema.yml (unique, not_null, relationships)

### P10. YAML Documentation is Thorough
The `_stg_wellview.yml` files provide descriptions for key columns, proper data_tests
on primary keys and foreign keys, and clear separation of refactored vs non-refactored
models.

---

## Recommended Action Priority

1. **H1** (audit column naming) -- batch rename in one commit, update YAML
2. **H2** (system lock varchar vs boolean) -- 2 files to fix
3. **H3** (system locking comment group) -- cosmetic, fix with H2
4. **M3** (proportion_to_pct macro) -- create macro, apply across 5 files
5. **M1+M2** (cost-per-foot, cost-per-hour macros) -- create 1-2 macros
6. **M4+M5** (force + torque macros) -- add to wellview_unit_conversions.sql
7. **L5** (record_id -> domain-specific name) -- rename in 3 staging models + update downstream refs
8. **L1-L4, L6** -- batch cleanup in a follow-up commit
