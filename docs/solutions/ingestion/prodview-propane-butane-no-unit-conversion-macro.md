---
title: "ProdView Propane/Butane Volumes Have No Unit Conversion Macro — Leave in Native m³"
category: ingestion
tags: [prodview, unit-conversion, propane, butane, macros, facility-monthly, m3]
module: operations/staging/prodview
symptoms:
  - Propane and butane volume columns appear in facility staging models in cubic meters (m³)
  - Project macro library has no pv_m3_to_gal_propane() or similar macro
  - Attempting to convert inline produces maintenance risk and undocumented magic numbers
date_solved: 2026-02-19
---

# ProdView Propane/Butane Volumes Have No Unit Conversion Macro

## Context

`stg_prodview__facility_monthly_volumes` (and related facility staging models) include propane and butane volume columns (e.g., `produced_propane_m3`, `receipts_in_propane_m3`, `opening_inventory_propane_m3`). All are stored in cubic meters (m³) natively in ProdView.

## Problem

The project macro library (`macros/prodview_helpers/prodview_unit_conversions.sql`) covers:
- `pv_cbm_to_bbl()` — cubic meters to barrels (oil/water)
- `pv_cbm_to_mcf()` — cubic meters to Mcf (gas)
- `pv_kpa_to_psi()` — pressure
- Temperature, choke, rate macros

It does NOT have macros for propane or butane conversion. Common propane/butane units (US gallons, barrels of LPG) require different conversion factors than oil/water/gas.

## Solution

**Leave propane and butane columns in native m³.** Document clearly in the staging and mart YAML:

```yaml
- name: produced_propane_m3
  description: >
    Propane production in cubic meters (m³). No project macro available for
    propane/butane unit conversion — remains in ProdView native unit.
    Do not convert inline without first adding an approved macro to
    macros/prodview_helpers/prodview_unit_conversions.sql.
```

The staging model already carries these correctly:

```sql
-- In stg_prodview__facility_monthly_volumes.sql
volprodpropane::float as produced_propane_m3,   -- intentionally in m³
volprodbutane::float as produced_butane_m3,      -- intentionally in m³
```

## When to Convert

If propane/butane values are needed in gallons or barrels of LPG:
1. Add a macro to `macros/prodview_helpers/prodview_unit_conversions.sql`:
   ```sql
   {% macro pv_m3_to_gal_propane(column_name) %}
       {{ column_name }} * 264.172052  -- 1 m³ = 264.172052 US gallons
   {% endmacro %}
   ```
2. Update staging models to use the macro (don't leave inline magic numbers).
3. Rename the column to drop the `_m3` suffix.

Do NOT create ad-hoc inline conversions in mart models — the staging layer is the conversion boundary.

## Prevention

1. When carrying a column with `_m3` suffix through to a mart, always check whether a conversion macro exists before deciding whether to convert or leave as-is.
2. If no macro exists, carry the column as-is with the `_m3` unit suffix to signal the native unit explicitly.
3. The `_m3` suffix in column names is the signal that conversion has NOT been applied.

## Related

- `macros/prodview_helpers/prodview_unit_conversions.sql` — full macro library
- `context/sources/prodview/entity_model.md` — Unit Conversions section
- `models/operations/staging/prodview/facilities/stg_prodview__facility_monthly_volumes.sql`
