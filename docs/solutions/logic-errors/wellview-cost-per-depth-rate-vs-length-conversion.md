---
title: "WellView Cost-Per-Depth Uses Wrong Unit Conversion Direction (10.76x Inflation)"
category: logic-errors
tags: [wellview, unit-conversion, cost-per-depth, rate-conversion, wv-macros, data-integrity]
module: operations/staging/wellview
symptoms:
  - Cost-per-foot values 10x higher than expected ($300-$3,200/ft instead of $28-$300/ft)
  - wv_meters_to_feet macro applied to $/meter columns instead of rate conversion
  - 14 cost_per_depth columns across 2 staging models inflated by ~10.76x
date_solved: 2026-02-13
---

# WellView Cost-Per-Depth Uses Wrong Unit Conversion Direction (10.76x Inflation)

## Problem

During Sprint 2 code review, 4 independent review agents flagged that `stg_wellview__job_reports` and `stg_wellview__job_program_phases` were using `wv_meters_to_feet()` for cost-per-depth columns. This inflated all cost-per-foot values by approximately 10.76x (the square of the feet-per-meter conversion factor).

Affected columns (14 total across 2 models):
- `cost_per_depth_per_ft`, `cumulative_cost_per_depth_per_ft`, `cost_per_depth_variance_per_ft`, `cost_per_depth_normalized_per_ft`
- `mud_cost_per_depth_per_ft`, `cumulative_mud_cost_per_depth_per_ft`, `mud_cost_per_depth_normalized_per_ft`, `cumulative_mud_cost_per_depth_normalized_per_ft`
- `cost_per_depth_drilled_per_ft`, `cost_per_depth_drilled_normalized_per_ft`, `planned_cost_per_depth_per_ft`, `planned_cost_per_depth_normalized_per_ft`
- `phase_mud_cost_per_depth_per_ft`, `phase_mud_cost_per_depth_normalized_per_ft`

## Root Cause

**Rate conversion vs. length conversion require opposite arithmetic.**

WellView stores all values in metric. When converting to imperial:

| Conversion type | Math | Peloton factor | Macro |
|----------------|------|----------------|-------|
| **Length** (meters to feet) | Divide by 0.3048 | `/ 0.3048` | `wv_meters_to_feet()` |
| **Rate** ($/meter to $/foot) | Multiply by 0.3048 (= divide by 3.28083989501312) | `/ 3.28083989501312` | `wv_per_meter_to_per_foot()` |

**Why the math is different:**

- Length: 1 meter = 3.28 feet, so to get feet you multiply by 3.28 (divide by 0.3048)
- Rate: If drilling costs $100/meter, it costs $100 per 3.28 feet, so per-foot cost = $100/3.28 = $30.48/ft

Using `wv_meters_to_feet()` on a rate column divides by 0.3048 instead of multiplying by it. The error factor is `(1/0.3048)^2 = 10.76x`.

**The subtle trap:** Peloton's conversion script (`wellview_conversions_from_db.txt`) lists both factors. The `/ 0.3048` factor appears for depth columns and the `/ 3.28083989501312` factor appears for cost-per-depth columns. Both are "dividing by a constant," so they look structurally similar — but they produce opposite results.

## Investigation Steps

1. Multi-agent code review caught the pattern independently across all 4 agents
2. Compared `stg_wellview__jobs.sql` (which correctly used `/ 3.28083989501312` inline) against the 2 broken models
3. Verified with `dbt show` that pre-fix values were $300-$3,200/ft (unreasonable) and post-fix values were $28-$300/ft (reasonable for US onshore drilling)

## Solution

### 1. Created new macro `wv_per_meter_to_per_foot`

```sql
-- In macros/wellview_helpers/wellview_unit_conversions.sql
{% macro wv_per_meter_to_per_foot(column_name) %}
    {{ column_name }} / 3.28083989501312
{% endmacro %}
```

### 2. Applied to all cost-per-depth columns

```sql
-- Before (WRONG — length conversion on a rate)
{{ wv_meters_to_feet('costperdepthcalc') }} as cost_per_depth_per_ft,

-- After (CORRECT — rate conversion)
{{ wv_per_meter_to_per_foot('costperdepthcalc') }} as cost_per_depth_per_ft,
```

### 3. Also updated `stg_wellview__jobs.sql`

Replaced 12 inline `/ 3.28083989501312` with the new macro for consistency:

```sql
-- Before (correct but inline magic number)
afepertargetdepthcalc / 3.28083989501312 as afe_per_target_depth_per_ft,

-- After (correct + uses macro)
{{ wv_per_meter_to_per_foot('afepertargetdepthcalc') }} as afe_per_target_depth_per_ft,
```

## Prevention

1. **Macro naming convention**: Rate-conversion macros use `wv_per_X_to_per_Y` naming to distinguish from length-conversion macros (`wv_X_to_Y`). The "per" prefix signals dimensional inversion.

2. **Code review checklist**: When reviewing WellView staging models, verify that:
   - Depth/distance columns use `wv_meters_to_feet()`
   - Cost-per-depth or rate-per-depth columns use `wv_per_meter_to_per_foot()`
   - Duration-per-depth columns also use `wv_per_meter_to_per_foot()`
   - Never mix length and rate macros on similar-looking column names

3. **Reasonableness check**: Always run `dbt show` and sanity-check values against domain knowledge. US onshore drilling costs typically range $15-$500/ft depending on well type and formation.

## Related

- `docs/solutions/refactoring/prodview-staging-5-cte-pattern.md` — ProdView staging refactor that established the 5-CTE pattern
- `macros/wellview_helpers/wellview_unit_conversions.sql` — Full macro library
- `context/sources/wellview/wellview_conversions_from_db.txt` — Peloton's original conversion factors
