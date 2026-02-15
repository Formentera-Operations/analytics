# Sprint 2 -- Inline Magic Numbers in Refactored Staging Models

These are unit conversion factors used inline in the 10 refactored staging models
that do NOT yet have a corresponding wv_* macro. Organized by conversion type.

## Cost Per Length ($/m -> $/ft)

Factor: `/ 3.28083989501312`
Suggested macro: `wv_per_m_to_per_ft()` (already exists for DLS, reuse or generalize)

| File | Line(s) | Count |
|------|---------|-------|
| stg_wellview__jobs.sql | 132-143 | 12 |

Note: stg_wellview__job_reports and stg_wellview__job_program_phases use
`wv_meters_to_feet()` for the same conversion, which is semantically different
(converts a length divisor, not a rate) but numerically equivalent due to the
reciprocal relationship. The inconsistency is worth resolving.


## Cost Per Hour ($/day -> $/hr)

Factor: `/ 24`
Suggested macro: Use `wv_days_to_hours()` which is `/ 0.0416666666666667` (= 1/24)

| File | Line(s) | Count |
|------|---------|-------|
| stg_wellview__jobs.sql | 126-129 | 4 |
| stg_wellview__job_reports.sql | 134-135 | 2 |


## Proportion to Percentage

Factor: `/ 0.01`
Suggested macro: `wv_proportion_to_pct()`

| File | Line(s) | Count |
|------|---------|-------|
| stg_wellview__jobs.sql | 206-212 | 7 |
| stg_wellview__job_reports.sql | 199-230 | 20 |
| stg_wellview__job_program_phases.sql | 154-158 | 5 |
| stg_wellview__job_afe_definitions.sql | 111 | 1 |
| stg_wellview__daily_costs.sql | 69 | 1 |
| **Total** | | **34** |


## Force (Newtons -> Klbf)

Factor: `/ 4448.2216152605` (= 4.4482216152605 * 1000)
Suggested macro: `wv_newtons_to_klbf()` or use `wv_newtons_to_lbf() / 1000`

| File | Line(s) | Count |
|------|---------|-------|
| stg_wellview__rigs.sql | 73-78 | 4 |


## Torque (N-m -> ft-lbf)

Factor: `/ 1.3558179483314`
Suggested macro: `wv_nm_to_ft_lbf()`

| File | Line(s) | Count |
|------|---------|-------|
| stg_wellview__rigs.sql | 81 | 1 |


## Temperature (Celsius -> Fahrenheit)

Factor: `/ 0.555555555555556 + 32`
Suggested macro: `wv_celsius_to_fahrenheit()`
Note: Used inline in stg_wellview__job_reports line 248. This factor appears in many
non-refactored models too (54+ occurrences across the wellview directory).

| File (refactored) | Line(s) | Count |
|------|---------|-------|
| stg_wellview__job_reports.sql | 248 | 1 |


## H2S (proportion -> PPM)

Factor: `/ 1E-06`
Suggested macro: `wv_proportion_to_ppm()`

| File | Line(s) | Count |
|------|---------|-------|
| stg_wellview__job_reports.sql | 209 | 1 |


## ROP Instantaneous (reciprocal conversion)

Factor: `power(nullif(col, 0), -1) / 0.00227836103820356`
This is a complex inverse + conversion. Macro candidate is low priority.

| File | Line(s) | Count |
|------|---------|-------|
| stg_wellview__job_program_phases.sql | 110 | 1 |


## Summary

| Conversion | Inline Count | Macro Priority |
|------------|-------------|----------------|
| Proportion to % | 34 | HIGH -- most widespread |
| Cost per length | 12 | MEDIUM |
| Cost per hour | 6 | MEDIUM |
| Temperature | 1 (refactored) / 54+ (total) | MEDIUM |
| Force | 4 | LOW |
| Torque | 1 | LOW |
| H2S to PPM | 1 | LOW |
| ROP inverse | 1 | LOW |
