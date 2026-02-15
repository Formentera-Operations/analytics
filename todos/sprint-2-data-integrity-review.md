# Sprint 2 WellView Staging Refactor -- Data Integrity Review

**Reviewed:** 2026-02-13
**Branch:** feature/sprint-1-drilling-dimensions
**Reviewer:** Data Integrity Guardian

## Summary

10 staging models refactored from 2-CTE to 5-CTE pattern. 2 models (well_header, jobs) had column renames from quoted display names to snake_case. 7 intermediate and 2 mart models updated as downstream consumers. Overall the refactor is well-executed with no broken references detected. Several issues of varying severity identified below.

---

## CRITICAL -- Unit Conversion Error (cost_per_depth in job_reports and job_program_phases)

**Severity:** CRITICAL -- silent data corruption
**Files:**
- `/Users/robstover/Development/formentera/analytics/analytics/models/operations/staging/wellview/operations/stg_wellview__job_reports.sql` (lines 130-139)
- `/Users/robstover/Development/formentera/analytics/analytics/models/operations/staging/wellview/operations/stg_wellview__job_program_phases.sql` (lines 132-151)

**Issue:** `cost_per_depth` columns in job_reports and job_program_phases use `wv_meters_to_feet()` (which divides by 0.3048), but in the jobs model the same type of column uses `/ 3.28083989501312` (which is the reciprocal: `1/0.3048 = 3.28083989501312`).

The conversions are **mathematically opposite**:

- `stg_wellview__jobs.sql` line 134: `costperdepthcalc / 3.28083989501312` -- This DIVIDES by ft/m, which is correct for cost/meter to cost/foot. If a value is $10/meter, dividing by 3.28 gives ~$3.05/foot. Correct.
- `stg_wellview__job_reports.sql` line 130: `{{ wv_meters_to_feet('costperdepthcalc') }}` -- This also DIVIDES by 0.3048 (the macro is `column / 0.3048`), which gives `$10 / 0.3048 = $32.81/foot`. Incorrect. Cost per foot should be LESS than cost per meter, not more.

The `wv_meters_to_feet` macro divides by 0.3048, which converts a LENGTH from meters to feet. But `cost_per_depth` is a RATE (dollars per unit length). To convert $/meter to $/foot, you must MULTIPLY by 0.3048 (or equivalently divide by 3.28...).

The jobs model does this correctly with its inline `/ 3.28083989501312` factor. The job_reports and job_program_phases models do it incorrectly by using the length conversion macro on a rate column.

**Impact:** cost_per_depth, cost_per_depth_variance, mud_cost_per_depth, and similar columns in job_reports and job_program_phases will be inflated by a factor of ~10.76x (0.3048^-2 instead of 0.3048^-1). This silently corrupts all cost-per-foot metrics coming from daily reports and phases.

**Affected columns in stg_wellview__job_reports:**
- `cost_per_depth_per_ft`
- `cumulative_cost_per_depth_per_ft`
- `cost_per_depth_variance_per_ft`
- `cost_per_depth_normalized_per_ft`
- `mud_cost_per_depth_per_ft`
- `cumulative_mud_cost_per_depth_per_ft`
- `mud_cost_per_depth_normalized_per_ft`
- `cumulative_mud_cost_per_depth_normalized_per_ft`

**Affected columns in stg_wellview__job_program_phases:**
- `cost_per_depth_drilled_per_ft`
- `cost_per_depth_drilled_normalized_per_ft`
- `planned_cost_per_depth_per_ft`
- `planned_cost_per_depth_normalized_per_ft`
- `phase_mud_cost_per_depth_per_ft`
- `phase_mud_cost_per_depth_normalized_per_ft`

**Fix:** Replace `{{ wv_meters_to_feet('costperdepthcalc') }}` with `costperdepthcalc / 3.28083989501312` (or create a dedicated `wv_per_meter_to_per_foot` macro). The Peloton script uses 1/3.28083989501312 = 0.3048 for this conversion because it is inverting the rate denominator.

Consider creating a macro like:
```sql
{% macro wv_per_meter_to_per_foot(column_name) %}
{# Convert rates in units/meter to units/foot: multiply by 0.3048 #}
    {{ column_name }} * 0.3048
{% endmacro %}
```

- [ ] Fix cost_per_depth conversions in stg_wellview__job_reports.sql
- [ ] Fix cost_per_depth conversions in stg_wellview__job_program_phases.sql
- [ ] Consider adding a wv_per_meter_to_per_foot macro for clarity

---

## HIGH -- Schema YAML Uses Legacy `tests:` Key (drilling marts)

**Severity:** HIGH -- deprecation warning / potential CI failure
**File:** `/Users/robstover/Development/formentera/analytics/analytics/models/operations/marts/drilling/schema.yml`

**Issue:** The drilling mart YAML file uses the legacy `tests:` key instead of the current `data_tests:` key. While this still works in dbt 1.x, it generates deprecation warnings and will break in dbt 2.0. The staging YAML files correctly use `data_tests:`.

**Inconsistency:** The staging YAMLs (general/_stg_wellview.yml, operations/_stg_wellview.yml, wellbore_surveys/_stg_wellview.yml) all use the modern `data_tests:` key. The drilling mart schema.yml uses the legacy `tests:` key. This inconsistency could cause confusion during future refactors.

- [ ] Update schema.yml to use `data_tests:` instead of `tests:` for all column test definitions

---

## HIGH -- Relationships Test Uses `arguments:` Syntax (drilling marts)

**Severity:** HIGH -- may cause test parse failure
**File:** `/Users/robstover/Development/formentera/analytics/analytics/models/operations/marts/drilling/schema.yml`

**Issue:** The `relationships` tests use `arguments:` nesting which is non-standard for modern dbt. The correct syntax places `to:` and `field:` directly under the test name:

Current (potentially broken):
```yaml
tests:
  - relationships:
      arguments:
        to: ref('well_360')
        field: eid
```

Should be:
```yaml
data_tests:
  - relationships:
      to: ref('well_360')
      field: eid
```

This affects:
- dim_job.eid -> well_360.eid
- dim_wellbore.eid -> well_360.eid
- dim_phase.job_sk -> dim_job.job_sk
- bridge_job_afe.job_sk -> dim_job.job_sk

- [ ] Fix all `relationships` test syntax in schema.yml (remove `arguments:` wrapper)

---

## MEDIUM -- int_wellview__well_header Overrides to `materialized='view'`

**Severity:** MEDIUM -- performance / convention
**File:** `/Users/robstover/Development/formentera/analytics/analytics/models/operations/intermediate/production/int_wellview__well_header.sql`

**Issue:** Per project conventions, intermediate models should default to `ephemeral` materialization (from dbt_project.yml). This model explicitly overrides to `view` with `config(enabled=true, materialized='view')`. The CLAUDE.md states this is legacy, not intentional. Same for `int_wellview_job.sql` and `int_fct_well_header.sql`.

However, this is a pre-existing condition from before the refactor and is not introduced by Sprint 2.

- [ ] No action required for Sprint 2 (pre-existing). Track for future cleanup.

---

## MEDIUM -- int_fct_well_header Still Uses Quoted Column Names

**Severity:** MEDIUM -- style inconsistency, not a breakage
**File:** `/Users/robstover/Development/formentera/analytics/analytics/models/operations/intermediate/production/int_fct_well_header.sql`

**Issue:** This model correctly consumes the new snake_case columns from `int_wellview__well_header` and `stg_wellview__well_header` but then re-aliases them back to quoted display names for downstream consumption (e.g., `w.well_id as "Well ID"`, `w.well_name as "Well Name"`). This is by design for the existing fct_eng_jobs mart which expects quoted column names.

This is not a bug -- it is a deliberate translation layer. However, it means the quoted-name pattern persists in the production mart (fct_eng_jobs). This should be tracked for eventual deprecation once all downstream consumers can switch to snake_case.

- [ ] No action required for Sprint 2. Track for future migration of fct_eng_jobs to snake_case.

---

## LOW -- Missing `_loaded_at` Watermark in Some Downstream Models

**Severity:** LOW -- no immediate impact
**Files:**
- `/Users/robstover/Development/formentera/analytics/analytics/models/operations/intermediate/production/int_wellview__well_header.sql`
- `/Users/robstover/Development/formentera/analytics/analytics/models/operations/intermediate/well_360/int_well__wellview.sql`
- `/Users/robstover/Development/formentera/analytics/analytics/models/operations/intermediate/well_360/int_well__spine.sql`

**Issue:** Several intermediate models that consume the refactored staging models do not carry through the `_loaded_at` timestamp. While these are materialized as views (so `_loaded_at` is accessible via the staging model), if they were ever switched to incremental materialization they would lack the watermark column needed for the `is_incremental()` filter.

- [ ] No action required for Sprint 2. Note for future incremental migration.

---

## VERIFIED -- Column Rename Cascade (PASS)

All 7 intermediate models and 2 mart models that consume `stg_wellview__well_header` and `stg_wellview__jobs` have been updated to reference the new snake_case column names. No dangling quoted column references were found in any downstream consumer of these staging models.

Verified models:
- `int_wellview_job.sql` -- references `job_id`, `well_id`, `job_category`, `well_type`, `cost_center`, `api_10_number` (all snake_case) -- PASS
- `int_wellview__well_header.sql` -- references `well_id`, `well_name`, `api_10_number`, `cost_center`, `eid`, `asset_company`, etc. -- PASS
- `int_fct_well_header.sql` -- correctly reads snake_case from int_wellview__well_header, re-aliases to quoted names for legacy mart -- PASS
- `int_well__wellview.sql` -- references `eid`, `well_id`, `well_name`, `api_10_number` as `api_10`, etc. -- PASS
- `int_well__spine.sql` -- references `eid`, `api_10_number` as `api_10` -- PASS
- `int_wellview__canonical_wells.sql` -- references `well_id`, `well_name`, `api_10_number`, `wellbore_name`, etc. -- PASS
- `fct_eng_jobs.sql` -- consumes `int_wellview_job` (not staging directly), all references valid -- PASS
- `dim_job.sql` -- references `job_id`, `well_id`, `wellbore_id`, `job_type_primary`, etc. -- PASS

---

## VERIFIED -- Fivetran Deduplication (PASS)

All 10 refactored staging models correctly implement the Fivetran dedup pattern:
```sql
qualify 1 = row_number() over (
    partition by idrec order by _fivetran_synced desc
)
```

**Note:** `stg_wellview__well_header` correctly partitions on `idwell` (not `idrec`) because the well header table's primary key is `IDWELL`, not `IDREC`. This is correct and intentional.

---

## VERIFIED -- Soft Delete Filtering (PASS)

All 10 refactored staging models correctly filter soft deletes in the `filtered` CTE:
```sql
where coalesce(_fivetran_deleted, false) = false
  and <primary_key> is not null
```

---

## VERIFIED -- Surrogate Key Generation (PASS)

All 10 refactored staging models generate surrogate keys using `dbt_utils.generate_surrogate_key` on the natural primary key:
- `stg_wellview__well_header`: `well_id`
- `stg_wellview__jobs`: `job_id`
- `stg_wellview__wellbores`: `record_id`
- `stg_wellview__job_reports`: `report_id`
- `stg_wellview__rigs`: `job_rig_id`
- `stg_wellview__job_time_log`: `time_log_id`
- `stg_wellview__daily_costs`: `cost_line_id`
- `stg_wellview__job_afe_definitions`: `job_afe_id`
- `stg_wellview__job_program_phases`: `record_id`
- `stg_wellview__job_interval_problems`: `record_id`

---

## VERIFIED -- Data Tests in YAML (PASS)

All 10 refactored staging models have:
- `unique` + `not_null` on their primary key
- `not_null` with `severity: warn` on foreign keys (well_id, job_id)

All 4 drilling mart models (dim_job, dim_wellbore, dim_phase, bridge_job_afe) have:
- `unique` + `not_null` on surrogate keys and natural keys
- `relationships` tests for foreign keys (though syntax needs fixing per HIGH finding above)

---

## VERIFIED -- Unit Conversion Macros (MOSTLY PASS)

The `wv_*` macros are applied correctly for length, volume, rate, density, and time conversions across all 10 models, with the critical exception of `cost_per_depth` noted above.

Verified conversions:
- `wv_meters_to_feet`: depths, elevations, distances -- correct
- `wv_meters_to_inches`: wellbore sizes, line IDs -- correct
- `wv_meters_to_miles`: town distances -- correct
- `wv_mps_to_ft_per_hr`: rates of penetration -- correct
- `wv_days_to_hours`: durations -- correct
- `wv_days_to_minutes`: short-duration sensor data -- correct
- `wv_kgm3_to_lb_per_gal`: mud density -- correct
- `wv_cbm_to_bbl`: volumes -- correct
- `wv_cbm_per_day_to_bbl_per_day`: production rates -- correct
- `wv_cbm_per_day_to_mcf_per_day`: gas rates -- correct
- `wv_kg_to_lb`: weight -- correct
- `wv_watts_to_hp`: power -- correct
- `wv_per_m_to_per_100ft`: dogleg severity -- correct
- `wv_meters_to_feet` on cost_per_depth: **INCORRECT** (see CRITICAL finding)

---

## Action Item Summary

| Priority | Issue | Files | Action |
|----------|-------|-------|--------|
| CRITICAL | cost_per_depth unit conversion wrong | job_reports, job_program_phases | Fix conversion factor (divide by 3.28 or multiply by 0.3048) |
| HIGH | Legacy `tests:` key in YAML | drilling/schema.yml | Change to `data_tests:` |
| HIGH | `arguments:` wrapper in relationships tests | drilling/schema.yml | Remove `arguments:` nesting |
| MEDIUM | View override on intermediate models | int_wellview_*.sql | Pre-existing; track for cleanup |
| MEDIUM | Quoted column names persist in fct_eng_jobs chain | int_fct_well_header.sql | Track for eventual snake_case migration |
| LOW | Missing _loaded_at in intermediates | Various int_ models | Note for future incremental migration |
