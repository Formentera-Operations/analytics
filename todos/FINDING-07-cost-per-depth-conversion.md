# FINDING-07: Cost-Per-Depth Conversion Discrepancy

**Status:** BLOCKING -- investigate before production merge
**Date:** 2026-02-13

## The Problem

Two refactored staging models convert "cost per depth" columns using different
math that produces opposite results.

### stg_wellview__jobs.sql (lines 131-143)

Uses `/ 3.28083989501312` for cost-per-foot columns:

```sql
-- cost per foot (per-meter -> per-foot: / 3.28083989501312)
afepertargetdepthcalc / 3.28083989501312 as afe_per_target_depth_per_ft,
costperdepthcalc / 3.28083989501312 as cost_per_depth_per_ft,
-- ... 10 more columns with this pattern
```

### stg_wellview__job_reports.sql (lines 130-138)

Uses `{{ wv_meters_to_feet('...') }}` (which divides by `0.3048`) for
similar cost-per-depth columns:

```sql
-- cost per unit (meters -> feet for cost/depth)
{{ wv_meters_to_feet('costperdepthcalc') }} as cost_per_depth_per_ft,
{{ wv_meters_to_feet('costperdepthcumcalc') }} as cumulative_cost_per_depth_per_ft,
-- ... more columns with this pattern
```

### stg_wellview__job_program_phases.sql (lines 132-135)

Also uses `{{ wv_meters_to_feet('...') }}` for cost-per-depth:

```sql
{{ wv_meters_to_feet('costperdepthcalc') }} as cost_per_depth_drilled_per_ft,
{{ wv_meters_to_feet('costperdepthnormcalc') }} as cost_per_depth_drilled_normalized_per_ft,
```

## The Math

The source column stores cost in $/meter (dollars per meter of depth).

To convert $/m to $/ft:

- 1 meter = 3.28083989501312 feet
- If it costs $100 per meter, it costs $100 / 3.28083989501312 = $30.48 per foot
- Formula: `value / 3.28083989501312` -- CORRECT for rate-per-distance conversion

What `wv_meters_to_feet` does:

- `value / 0.3048` = multiply by 3.28083989501312
- $100 / 0.3048 = $328.08 per foot -- WRONG for rate-per-distance

## Investigation Results -- CONFIRMED BUG

Peloton's raw conversion script (`wellview_unit_conversions_raw.txt`) confirms
that ALL cost-per-depth columns use `/ 3.28083989501312`:

```
"COSTPERDEPTHCALC"/3.28083989501312 AS "COSTPERDEPTHCALC"  -- line 7215
"COSTPERDEPTHCUMCALC"/3.28083989501312 AS "COSTPERDEPTHCUMCALC"  -- line 7216
"MUDCOSTPERDEPTHCALC"/3.28083989501312 AS "MUDCOSTPERDEPTHCALC"  -- line 7327
```

This confirms:
- **stg_wellview__jobs.sql** is CORRECT (uses `/ 3.28083989501312`)
- **stg_wellview__job_reports.sql** is WRONG (uses `wv_meters_to_feet` which
  divides by `0.3048` -- producing values ~10.76x too large)
- **stg_wellview__job_program_phases.sql** is WRONG (same error as job_reports)

## Affected Columns

### stg_wellview__job_reports.sql (8 columns wrong)
- `cost_per_depth_per_ft` (line 130)
- `cumulative_cost_per_depth_per_ft` (line 131)
- `cost_per_depth_variance_per_ft` (line 132)
- `cost_per_depth_normalized_per_ft` (line 133)
- `mud_cost_per_depth_per_ft` (line 136)
- `cumulative_mud_cost_per_depth_per_ft` (line 137)
- `mud_cost_per_depth_normalized_per_ft` (line 138)
- `cumulative_mud_cost_per_depth_normalized_per_ft` (line 139)

### stg_wellview__job_program_phases.sql (6 columns wrong)
- `cost_per_depth_drilled_per_ft` (line 132)
- `cost_per_depth_drilled_normalized_per_ft` (line 133)
- `planned_cost_per_depth_per_ft` (line 134)
- `planned_cost_per_depth_normalized_per_ft` (line 135)
- `phase_mud_cost_per_depth_per_ft` (line 150)
- `phase_mud_cost_per_depth_normalized_per_ft` (line 151)

## Fix Required

1. Add macro to `macros/wellview_helpers/wellview_unit_conversions.sql`:

```sql
{% macro wv_per_m_to_per_ft(column_name) %}
{# Peloton factor: / 3.28083989501312 | cost/m -> cost/ft (COST/FT) #}
    {{ column_name }} / 3.28083989501312
{% endmacro %}
```

2. Replace `{{ wv_meters_to_feet('...') }}` with `{{ wv_per_m_to_per_ft('...') }}`
   on the 14 affected columns in the two models.

3. Also replace the 12 inline `/ 3.28083989501312` in stg_wellview__jobs.sql
   with the new macro for consistency.

The `wv_meters_to_feet` macro should ONLY be used for absolute length
conversions (depth, elevation, distance), never for rate-per-distance columns.
