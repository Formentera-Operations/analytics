---
title: "WellView Staging Domain Refactor — Sprint 2 Patterns and Learnings"
category: refactoring
tags: [wellview, staging, 5-cte, domain-organization, split-source, snake-case, downstream-cascade, wv-macros, sprint-2]
module: operations/staging/wellview
symptoms:
  - 58 staging models flat in single directory with no organization
  - 2-CTE pattern (source, renamed) instead of project-standard 5-CTE
  - Quoted display name aliases in well_header and jobs cascade to all consumers
  - No schema YAML documentation or tests on WellView models
  - Inline magic numbers for unit conversions instead of wv_* macros
date_solved: 2026-02-13
---

# WellView Staging Domain Refactor — Sprint 2 Patterns and Learnings

## Problem

WellView had 58 staging models all in a single flat directory with no domain organization, no YAML documentation, no tests, and using a legacy 2-CTE pattern. Two critical models (`stg_wellview__well_header` and `stg_wellview__jobs`) used quoted display-name aliases that cascaded through all downstream intermediates and marts.

## What Sprint 2 Accomplished

- Reorganized 26 models into 3 domain subdirectories (`general/`, `wellbore_surveys/`, `operations/`)
- Refactored 10 models from 2-CTE to project-standard 5-CTE pattern
- Converted 2 models from quoted display names to snake_case with full downstream cascade (5 intermediates, 2 marts)
- Added per-domain `_src_wellview.yml` (split-source pattern) and `_stg_wellview.yml` (model docs + tests)
- Replaced inline magic numbers with `wv_*` macros across all 10 refactored models
- Created `wv_per_meter_to_per_foot` and `wv_per_m_to_per_100ft` macros

## Key Patterns Established

### 1. Split-Source YAML Pattern

Each domain subdirectory gets its own `_src_wellview.yml` declaring tables for the same source:

```yaml
# models/operations/staging/wellview/operations/_src_wellview.yml
sources:
  - name: wellview_calcs
    database: PELOTON_FORMENTERAOPS_FORMENTERAOPS_WV120
    schema: FORMENTERAOPS_WV120_CALC
    tables:
      - name: WVT_WVJOB
      - name: WVT_WVJOBREPORT
      # ... domain-specific tables only
```

dbt merges tables from all files sharing the same source name. The root `src_wellview_calcs.yml` retains tables not yet assigned to a domain subdirectory.

### 2. Downstream Cascade Rules

When staging columns change from quoted to snake_case:

| Layer | Output naming | Rule |
|-------|--------------|------|
| **Staging** | snake_case | Clean internal contract |
| **Intermediate** | snake_case | Update refs from quoted to snake_case |
| **Marts** | Preserve quoted aliases | `j.job_id as "Job ID"` — dashboards reference these |

### 3. Unit Conversion Macro Categories

Two distinct conversion categories — never confuse them:

| Category | Example | Macro pattern | Math |
|----------|---------|---------------|------|
| **Length/volume** | meters to feet | `wv_meters_to_feet()` | Divide by 0.3048 |
| **Rate** | $/meter to $/foot | `wv_per_meter_to_per_foot()` | Divide by 3.28... |

The "per" prefix in macro names signals a rate conversion. See `logic-errors/wellview-cost-per-depth-rate-vs-length-conversion.md` for the full explanation.

### 4. System Field Type Casting Consistency

All WellView staging models must use consistent types:

| Field pattern | Type | Notes |
|--------------|------|-------|
| `syslockme*` | `::boolean` | Lock flags, not strings |
| `sysseq` | `::int` | Sequence ordinals, not floats |
| `syscreatedate` / `sysmoddate` | `::timestamp_ntz` | UTC timestamps |
| `syscreateuser` / `sysmoduser` | `trim(...)::varchar` | User identifiers |

### 5. YAML Test Configuration

Use `config:` wrapper for test properties (required in dbt 1.11+):

```yaml
# Correct
data_tests:
  - not_null:
      config:
        severity: warn

# Deprecated (fails CI --warn-error)
data_tests:
  - not_null:
      severity: warn
```

Always use `data_tests:` (not `tests:`).

## Execution Strategy

Sprint 2 used a swarm approach with 6 parallel agents:

1. **Agent 1**: `stg_wellview__well_header` + 5 WellView intermediates + 1 production intermediate
2. **Agent 2**: `stg_wellview__jobs` + `dim_job` + `int_wellview_job` + `fct_eng_jobs`
3. **Agent 3**: `stg_wellview__wellbores` + `stg_wellview__job_program_phases` + `stg_wellview__job_afe_definitions`
4. **Agent 4**: `stg_wellview__job_reports` + `stg_wellview__job_time_log` + `stg_wellview__daily_costs`
5. **Agent 5**: `stg_wellview__rigs` + `stg_wellview__job_interval_problems` + new macros
6. **Agent 6**: Directory structure + git mv + YAML files (ran first, others depended on it)

**Key dependency**: Agent 6 (file moves) must complete before Agents 1-5 (file edits) begin. Agents 1 and 2 had a shared dependency on `int_wellview_job.sql` — resolved by having Agent 1 handle well_header refs first, then Agent 2 handle jobs refs.

## Gotchas Encountered

1. **`git mv`'d files inherit pre-existing lint issues** — SQLFluff runs on all staged files, including moved-but-not-modified ones. Run `sqlfluff fix` after moves.

2. **Partial parse hides deprecation warnings** — Always verify YAML changes with `dbt parse --warn-error --no-partial-parse` before pushing.

3. **`int_wellview_job.sql` has both well_header and jobs dependencies** — When refactoring both staging models simultaneously, coordinate the cascade carefully to avoid merge conflicts.

4. **Root `src_wellview_calcs.yml` must be updated** — Remove table declarations that move to domain `_src_wellview.yml` files to avoid duplicate source definitions.

## Verification Checklist

```bash
# 1. Parse (full, no cache)
dbt parse --warn-error --no-partial-parse

# 2. Build all modified + downstream
dbt build --select stg_wellview__well_header+ stg_wellview__jobs+ ...

# 3. Spot-check values
dbt show --select stg_wellview__job_reports --limit 5
# Verify cost_per_depth values are in reasonable range ($15-$500/ft)

# 4. Lint
sqlfluff lint models/operations/staging/wellview/
yamllint -c .yamllint.yml models/operations/staging/wellview/**/*.yml

# 5. Downstream tests
dbt test --select dim_job dim_wellbore dim_phase bridge_job_afe well_360 fct_eng_jobs
```

## Related

- `docs/solutions/logic-errors/wellview-cost-per-depth-rate-vs-length-conversion.md` — Critical bug found during this sprint
- `docs/solutions/build-errors/ci-dbt-parse-missing-arguments-deprecation.md` — CI failure found during this sprint
- `docs/solutions/refactoring/prodview-staging-5-cte-pattern.md` — ProdView refactor that established the 5-CTE pattern
- `docs/solutions/refactoring/wellview-eid-resolution-via-well-360.md` — EID resolution pattern from Sprint 1
- `docs/solutions/build-errors/ci-defer-stale-column-names.md` — CI --defer interaction with column renames
- `docs/plans/wellview-calc-blueprint-plan.md` — Full Sprint 2 implementation plan
