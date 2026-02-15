# Intermediate Model Conventions

## Overview

Intermediate models transform staging data into shapes ready for marts. An intermediate model exists to isolate a **reusable** transformation that would otherwise be duplicated or create an unreadably complex mart.

- **Materialization:** `ephemeral` (inherited from `dbt_project.yml` — do NOT override unless you have a specific reason)
- **Naming:** `int_{domain}__{description}` (e.g., `int_gl_enhanced`, `int_griffin__daily_production`)
- **Location:** `models/operations/intermediate/{domain}/`

> **Note:** Many existing intermediate models override to `view` — this is legacy, not intentional. New models should use the ephemeral default. Use `dbt show` to preview/debug ephemeral models.

## When to Create an Intermediate Model

1. **Multi-source join** — Combining 2+ staging models into one enriched dataset (e.g., `int_prodview__production_volumes` joins allocations + downtimes + parameters + status)
2. **Entity spine** — Creating an authoritative list of entities across source systems (e.g., `int_well__spine` unifies well EIDs from ODA, ProdView, WellView, Combo Curve)
3. **Business classification** — Applying domain rules that multiple marts need (e.g., `int_accounts_classified` maps account codes to LOS categories, interest types, expense classifications)
4. **Aggregation or reshape** — Changing grain or pivoting data before it reaches marts (e.g., `int_oda_ar_invoice_payments_agg`)
5. **Source enrichment** — Adding dimensional context to a source before mart consumption (e.g., `int_griffin__completions_enriched`)

## When NOT to Create an Intermediate Model

- The transformation is only used by one mart — put it in CTEs within that mart instead
- You're just renaming or filtering — that belongs in staging
- You're building a final business entity (dimension or fact) — that's a mart

## Materialization Guidance

| Materialization | When to Use | Example |
|----------------|-------------|---------|
| `ephemeral` (default) | Most intermediate models. Compiles as a CTE into downstream models, no Snowflake object created. | Most `int_*` models |
| `table` | Expensive logic (complex CASE chains, many joins) AND referenced by 3+ downstream models | `int_accounts_classified` |
| `incremental` | Very large datasets that can't be fully rebuilt each run | `int_gl_enhanced`, `int_general_ledger_enhanced` (merge on `_loaded_at`) |

Only add an explicit `config(materialized=...)` block when overriding the default — ephemeral models need no config block.

## Debugging Ephemeral Models

Ephemeral models don't create Snowflake objects, so you can't query them directly. Use:

```bash
dbt show --select int_model_name --limit 10
```

This compiles the model inline and runs it, showing you the output without materializing it.
