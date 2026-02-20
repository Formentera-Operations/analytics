# GL Intermediate Consolidation & fct_gl_details

**Date:** 2026-02-20
**Status:** Ready for planning
**Scope:** Finance intermediate + mart layer restructuring

## What We're Building

Converge two parallel GL intermediate models (`int_general_ledger_enhanced` and `int_gl_enhanced`) into a single canonical intermediate, then materialize a core `fct_gl_details` mart table that serves as the single source of truth for all GL-derived reporting.

### Current State (Problems)

| Model | Rows | Dupes | Watermark | Consumers |
|-------|------|-------|-----------|-----------|
| `int_general_ledger_enhanced` | 180.7M | 524 | `created_at`/`updated_at` double-subquery (fragile) | `general_ledger`, `fct_eng_GL` |
| `int_gl_enhanced` | 180.6M | 0 | `_loaded_at` (always current_timestamp — no selectivity) | `fct_los` |
| Source (`stg_oda__gl`) | 180.8M | — | CDC via Estuary, `_flow_published_at` available | — |

**Root issues:**
1. Two models doing the same 15-table enrichment with different quality levels
2. Neither has a functional incremental watermark (one scans full source every run, the other uses a fragile double-subquery)
3. Old model bakes presentation formatting into the intermediate layer (`'Y'`/`'N'`, `'MM-DD-YYYY'` strings, prefixed entity codes)
4. Old model produces 524 duplicate gl_ids
5. No YAML documentation for either model

### Target State

```
stg_oda__gl (view, CDC)
    └── int_gl_enhanced (incremental, _flow_published_at watermark)
            └── fct_gl_details (incremental, canonical GL fact in marts)
                    ├── fct_los (incremental, LOS-filtered view)
                    ├── fct_eng_GL (table, Power BI quoted-alias wrapper)
                    └── [future consumers]
```

## Why This Approach

### Single intermediate, single fact, multiple views

- **int_gl_enhanced** already has the correct design: consolidated polymorphic CTEs, clean analytical types, proper FK IDs, no presentation formatting
- **fct_gl_details** materializes the enriched GL as a table in `FO_PRODUCTION_DB.marts` — the canonical GL fact
- Derivative models (`fct_los`, `fct_eng_GL`) become thin wrappers that filter and alias from `fct_gl_details`
- New GL consumers can derive from `fct_gl_details` without touching the intermediate

### Fix the watermark once

- Switch from `_loaded_at` (= `current_timestamp()`, zero selectivity) to `_flow_published_at` (Estuary CDC timestamp)
- Validated: 100% populated, 180.8M rows, 20 days of CDC history, monotonically increasing
- True incremental: only process rows ingested since last run
- Propagate `_flow_published_at` through `int_gl_enhanced` → `fct_gl_details` so downstream incrementals also benefit

## Key Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Surviving intermediate | `int_gl_enhanced` | Better design (consolidated CTEs, clean types, no dupes, proper FKs) |
| Watermark column | `_flow_published_at` | Only reliable CDC timestamp; `_loaded_at` = current_timestamp() on views |
| fct_gl_details materialization | Incremental (merge) | 180M rows; true incremental now possible with fixed watermark |
| fct_eng_GL location | Keep in `marts/production/` | Feeds live Power BI dashboard; avoid breaking connection string |
| fct_eng_GL aliasing | Preserve `"Quoted Column Names"` | Production Power BI dependency — refactoring aliases would break dashboard |
| general_ledger + lease_operating_statement | Delete both | `fct_los` covers the LOS use case with dimension-driven account filtering |
| los_v5_* models (4) | Delete | Legacy LOS API mappings, superseded by `fct_los` |
| YAML documentation | Add to `_int_oda_finance.yml` and `schema.yml` | Neither GL intermediate nor fct_gl_details has docs today |

## Models to Create

### fct_gl_details (new)
- **Location:** `models/operations/marts/finance/fct_gl_details.sql`
- **Materialization:** incremental (merge on `gl_id`)
- **Cluster:** `['company_code', 'journal_date', 'main_account', 'sub_account']`
- **Watermark:** `_flow_published_at`
- **Content:** `SELECT * FROM int_gl_enhanced` + `dim_accounts` join for account classification (account_category, financial_statement_line)
- **Purpose:** Canonical GL fact — all enrichments, clean types, proper FKs, account hierarchy

## Models to Modify

### int_gl_enhanced
- Fix watermark from `_loaded_at` to `_flow_published_at`
- Ensure `_flow_published_at` is passed through to final select
- No structural changes needed — design is already correct

### fct_los
- Repoint from `int_gl_enhanced` to `fct_gl_details`
- Simplify: account classification columns now come from `fct_gl_details` instead of separate `dim_accounts` join
- Keep LOS-specific sign-flipping logic (`is_los_subtraction`)

### fct_eng_GL
- Repoint from `int_general_ledger_enhanced` to `fct_gl_details`
- Map new column names to existing `"Quoted Column Name"` aliases
- Keep `dim_accounts` join for LOS account filtering (same as current)
- Keep all existing column aliases verbatim — Power BI dataset contract

## Models to Delete

| Model | Why |
|-------|-----|
| `int_general_ledger_enhanced` | Superseded by `int_gl_enhanced` |
| `general_ledger` | Superseded by `fct_gl_details` |
| `lease_operating_statement` | Superseded by `fct_los` (hardcoded account ranges vs dimension-driven) |
| `los_v5_balance_sheet` | Legacy, superseded by `fct_los` |
| `los_v5_revenue` | Legacy, superseded by `fct_los` |
| `los_v5_expense` | Legacy, superseded by `fct_los` |
| `los_v5_production` | Legacy, superseded by `fct_los` |

## Post-Deploy Steps

1. `dbt run --select int_gl_enhanced --full-refresh` — rebuild with new `_flow_published_at` watermark
2. `dbt run --select fct_gl_details --full-refresh` — initial materialization of new canonical fact
3. Snowflake cleanup:
   ```sql
   DROP TABLE IF EXISTS FO_PRODUCTION_DB.MARTS.GENERAL_LEDGER;
   DROP TABLE IF EXISTS FO_PRODUCTION_DB.MARTS.LEASE_OPERATING_STATEMENT;
   DROP TABLE IF EXISTS FO_STAGE_DB.INTERMEDIATE.INT_GENERAL_LEDGER_ENHANCED;
   -- los_v5 drops (verify table names in Snowflake first)
   ```
4. Validate Power BI `fct_eng_GL` dashboard still renders correctly

## Resolved Questions

1. **rev_deck_sets**: Leave out of `int_gl_enhanced`. No consumer uses the revenue deck set code. YAGNI.
2. **dim_accounts join in fct_gl_details**: **Include it.** `fct_gl_details` joins `dim_accounts` so every GL row has account classification (account_category, is_los_account, los_category, commodity_type, etc.). Makes `fct_los` a pure filter + sign-flip. Makes `fct_eng_GL` a pure alias wrapper.
3. **NRI columns**: **Add to `int_gl_enhanced`.** The `rev_deck_revisions` join already exists — just select `total_interest_expected` and `nri_actual` as two additional columns. Keeps `fct_eng_GL` as a thin alias layer.

## Watermark Fix Discovery

`stg_oda__gl` is a VIEW with `_loaded_at = current_timestamp()`. This means the existing `int_gl_enhanced` incremental watermark (`WHERE _loaded_at > max(_loaded_at)`) has **zero selectivity** — every run does a full 180M row scan.

**Fix:** Switch watermark to `_flow_published_at` (Estuary CDC timestamp).
- 100% populated across all 180.8M rows
- Range: 2026-01-29 to 2026-02-20 (20 days of CDC history)
- Monotonically increasing within each CDC capture batch
- Gives true incremental behavior: only process rows ingested since last run

## Scope Estimate

- ~7 models deleted
- ~1 model created (fct_gl_details)
- ~3 models modified (int_gl_enhanced, fct_los, fct_eng_GL)
- YAML additions for int_gl_enhanced and fct_gl_details
- 2 sprints: Sprint 1 = create + repoint, Sprint 2 = delete + cleanup
