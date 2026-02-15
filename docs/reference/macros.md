# Macro Library

Use these instead of writing inline SQL. They handle edge cases already.

## General Macros

| Macro | Purpose | Example |
|-------|---------|---------|
| `clean_null_string(col)` | `NULLIF(TRIM(col), '')` — converts empty strings to NULL | `{{ clean_null_string('description') }}` |
| `clean_null_int(col)` | `NULLIF(col, 0)` — converts 0 to NULL (Procount FK convention) | `{{ clean_null_int('gatheringsystemid') }}` |
| `standardize_date(col)` | Handles NULL-equivalent dates (`1900-01-01`, `1899-12-31`) and casts to DATE | `{{ standardize_date('effectivedate') }}` |
| `standardize_timestamp(date_col, time_col)` | Combines separate date/time columns into TIMESTAMP | `{{ standardize_timestamp('userdatestamp', 'usertimestamp') }}` |
| `parse_excel_date(col)` | Converts Excel serial date numbers to DATE (handles leap year bug) | `{{ parse_excel_date('startdate') }}` |
| `is_date_effective(start, end)` | Returns BOOLEAN for date-range validity checks | `{{ is_date_effective('startdate', 'enddate') }}` |
| `generate_surrogate_key(fields)` | MD5 hash of concatenated fields | `{{ generate_surrogate_key(['merrickid', 'type']) }}` |
| `set_warehouse_size(size)` | Routes to env-appropriate warehouse (XS/S/M) | `{{ config(snowflake_warehouse=set_warehouse_size('M')) }}` |
| `transform_company_name(col)` | Standardizes company name variations | `{{ transform_company_name('company_name') }}` |
| `transform_reserve_category(col)` | Maps reserve classification codes | `{{ transform_reserve_category('reserve_cat') }}` |

## Source-Specific Macros

### Procount (`macros/procount_helpers/`)

| Macro | Purpose |
|-------|---------|
| `decode_object_type(col)` | Maps Procount object type codes to readable names |
| `generate_object_key(fields)` | Builds composite key for Procount objects |
| `object_type_case(col)` | CASE expression for object type classification |

### ProdView (`macros/prodview_helpers/`)

| Macro | Purpose |
|-------|---------|
| `prodview_unit_conversions` | Unit conversion macros: `pv_cbm_to_bbl`, `pv_cbm_to_mcf`, `pv_days_to_hours`, `pv_decimal_to_pct`, `pv_kpa_to_psi`, `pv_joules_to_mmbtu` |

### WellView (`macros/wellview_helpers/`)

| Macro | Purpose |
|-------|---------|
| `wellview_unit_conversions` | Unit conversion macros: `wv_meters_to_feet`, `wv_meters_to_inches`, `wv_kpa_to_psi`, `wv_days_to_hours`, `wv_cbm_per_sec_to_gpm` |

## Adding New Macros

Place macros in the appropriate directory under `macros/`. Document them in `macros/macros.yml` with a description. Do not add macros without discussing with the team first.
