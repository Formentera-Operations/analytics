---
title: "Replacement Mart Silently Drops Columns from Legacy Fact — Caught by Codex PR Review"
category: build-errors
tags: [codex, pr-review, migration, gas-equivalent, tank-inventory, column-parity, replacement-fact]
module: operations/marts/well_360
symptoms:
  - New mart model missing gas-equivalent volume columns that existed in legacy fact
  - dbt build and all tests pass — the omission produces no test failure
  - Downstream consumers migrating from fct_eng_* to new fact silently lose metrics
  - Codex PR review flags: "downstream migration will lose those metrics"
date_solved: 2026-02-19
---

# Replacement Mart Silently Drops Columns from Legacy Fact — Caught by Codex PR Review

## Problem

`fct_tank_inventory_daily` was built as a replacement for `fct_eng_tank_inventories`. The new model included all the core volume columns but inadvertently omitted three `*_gas_equivalent_oil_cond_volume_mcf` columns:

- `opening_gas_equivalent_oil_cond_volume_mcf`
- `closing_gas_equivalent_oil_cond_volume_mcf`
- `change_gas_equivalent_oil_cond_volume_mcf`

These columns existed in both `stg_prodview__tank_daily_volumes` (the staging source) and the legacy `fct_eng_tank_inventories` fact.

## Discovery

The omission was caught by the **Codex automated PR review** (`chatgpt-codex-connector`) with a P2 severity inline comment:

> "fct_tank_inventory_daily is described as a functional replacement for fct_eng_tank_inventories, but this select drops the `*_gas_equivalent_oil_cond_volume_mcf` measures that exist in stg_prodview__tank_daily_volumes and were exposed by the legacy model."

No dbt test caught this — column omissions from a new model produce no test failure (tests only validate what's present, not what's absent).

## Fix

Add the three missing columns to every CTE in the model chain (not just the final CTE):

```sql
-- 1. In the tank_volumes source CTE (SELECT from staging):
opening_gas_equivalent_oil_cond_volume_mcf,
closing_gas_equivalent_oil_cond_volume_mcf,
change_gas_equivalent_oil_cond_volume_mcf,

-- 2. In the volumes_with_eid pass-through CTE:
v.opening_gas_equivalent_oil_cond_volume_mcf,
v.closing_gas_equivalent_oil_cond_volume_mcf,
v.change_gas_equivalent_oil_cond_volume_mcf,

-- 3. In the final output CTE:
opening_gas_equivalent_oil_cond_volume_mcf,
closing_gas_equivalent_oil_cond_volume_mcf,
change_gas_equivalent_oil_cond_volume_mcf,
```

Add corresponding descriptions to `schema.yml`.

## Prevention

**When building a replacement fact for a legacy model, diff the column lists explicitly:**

1. Extract columns from legacy model:
   ```bash
   grep -E "^\s+[a-z_]+ as \"" models/operations/marts/production/fct_eng_tank_inventories.sql
   ```

2. Extract columns from new model:
   ```bash
   grep -E "^\s+[a-z_]+," models/operations/marts/well_360/fct_tank_inventory_daily.sql
   ```

3. Compare and identify any omissions.

**Check specifically for these commonly-overlooked column groups:**
- `*_gas_eq*` or `*_gas_equivalent*` — gas equivalent volume columns
- `*_bsw*` — basic sediment and water quality percentages
- `cum_*` — cumulative production columns
- `*_sand_*` — sand volume columns (often low-value but should be carried for completeness)

## Larger lesson: Automated PR review catches real gaps

Review Codex PR comments (`chatgpt-codex-connector`) on every PR before merging. In this case it caught a genuine functional regression that:
- Passed all dbt tests
- Passed sqlfluff lint
- Passed yamllint
- Was invisible without comparing against the legacy model

Automated reviewers excel at spotting "what's missing" — a pattern that test suites can't validate.

## Related

- `models/operations/marts/well_360/fct_tank_inventory_daily.sql`
- `models/operations/marts/production/fct_eng_tank_inventories.sql` (legacy)
- `models/operations/staging/prodview/tanks/stg_prodview__tank_daily_volumes.sql`
