---
title: "refactor: ODA Staging Refactor — Sprint 5: Accounts Receivable (10 models) + GL"
type: refactor
date: 2026-02-17
deepened: 2026-02-17
linear_issues:
  - FOR-280  # AR CDC (2 models)
  - FOR-281  # AR batch (7 models)
  - FOR-282  # AR YAML + downstream fixes
milestone: "Sprint 5: Accounts Receivable (9 models)"
branch: feature/oda-staging-refactor-sprint-5-ar
---

# ♻️ ODA Staging Refactor — Sprint 5: Accounts Receivable + GL

## Enhancement Summary

**Deepened on:** 2026-02-17
**Research agents used:** data-integrity-guardian, architecture-strategist, performance-oracle,
deployment-verification-agent, data-migration-expert, learnings-researcher

### Key Improvements from Research

1. **GL surrogate key must NOT be composite or present at view level** — 3 independent agents
   agree: `generate_surrogate_key` at query time on 180M rows adds MD5 overhead on every
   consumer query. Remove the SK from `stg_oda__gl`. Add `_loaded_at` only. GL downstream
   joins use natural key `id` (aliased as `gl_id`).
2. **Volume columns need `decimal(19,4)` not `decimal(18,2)`** — `stg_oda__arinvoicenetteddetail`
   has `net_volume_non_working`, `net_volume_working` columns that represent fractional BOE/MCF
   units. Cast them as `decimal(19,4)` to match existing GL convention.
3. **GL business logic (description normalization) belongs in enhanced, not renamed CTE** —
   move the `CASE WHEN TRIM(DESCRIPTION) IN ('.',''...)` expression from `renamed` to `enhanced`.
   The `renamed` CTE should contain only type casts and renames.
4. **GL `company_id` FK test must be `severity: warn`** — defaults to `error` in current
   `schema.yml`. With only 39 companies, one orphan fails CI and blocks all GL builds.
5. **Pre-deploy CDC baseline is mandatory** — document exact soft-deleted invoice count in the
   PR description before merging. The row count drop in `stg_oda__arinvoice_v2` will look like
   data loss without this documented baseline.
6. **4 intermediate models filter on `i.Posted = 1`** — must ALL be updated to `i.is_posted`
   before merge. These are compile-time successes but runtime failures.
7. **Inner-join-after-left-join anti-pattern** — review all AR consumers for this pattern.
   NULL FKs in staging (e.g., `well_id`) pass through left joins but get dropped by inner joins.
8. **`schema.yml` GL model definition must move** to `general_ledger/_stg_oda__general_ledger.yml`.

### New Risks Discovered

- **`aradvancecloseout` voucher FK direction may be inverted** in `int_oda_ar_advance_closeout_pairs`
  — flag for verification when rows appear. The context doc and intermediate use opposite naming.
- **`int_oda_gl.sql` is a view scanning 180M rows twice** — separate follow-up sprint item to
  convert to table. Not in scope for Sprint 5 but document as tech debt.
- **CI defer stale column names** — after AR column renames merge, subsequent PRs touching
  AR downstream consumers may fail CI until the next prod dbt run.

---

## Overview

Sprint 5 completes the ODA staging refactor project (46/46 models). This sprint refactors 9
Accounts Receivable staging models from the old 2-CTE format to the standard 5-CTE pattern, plus
fully refactors and moves `stg_oda__gl` to its domain directory. After this sprint, `src_oda.yml`
will be completely empty (all 46 tables in domain-specific YAML files).

**Critical discovery**: `stg_oda__gl.sql` is in the **old 2-CTE format** (not fully refactored as
the original spec stated). It uses `tags=['staging', 'oda', 'gl']`, has no surrogate key, puts
`_loaded_at` in the renamed CTE, and uses `select * from renamed where _operation_type != 'd'`
instead of a proper `filtered` CTE. It needs a **full 5-CTE refactor**, not just a directory move.

---

## Problem Statement

36 of 46 ODA staging models have been refactored (Sprints 0–3b). The remaining 10:
- Do not filter CDC soft deletes → leaking deleted records to downstream consumers
- Have no config blocks → no materialization, no tags
- Use `select *` → no explicit column contracts
- Use PascalCase column names from raw source → inconsistent with snake_case convention
- Have no surrogate keys → no stable join key for downstream marts
- Are in the flat `oda/` directory → not organized by domain
- Have no YAML documentation → no tests, no column descriptions

After this sprint, `src_oda.yml` will be empty — a milestone that represents 100% completion
of the ODA staging refactor project.

---

## Models in Scope

### GL Move + Full Refactor (1 model)

| Model | Source Table | Rows | Connector | Notes |
|-------|-------------|------|-----------|-------|
| `stg_oda__gl` | `GL` | 180M | **CDC** | Currently 2-CTE, wrong tags, no SK, no filtered/enhanced/final CTEs |

**Important**: Despite the original spec saying "DO NOT modify the SQL", the file is in the old
2-CTE format. It **must** be fully refactored to pass `validate_staging.py`. The GL already has
`_src_oda__general_ledger.yml` and `_stg_oda__general_ledger.yml` in the `general_ledger/`
directory — just add GL's source entry and staging YAML column docs there.

### AR CDC (2 models)

| Model | Source Table | Rows | Connector | CDC Filter |
|-------|-------------|------|-----------|-----------|
| `stg_oda__arinvoice_v2` | `ODA_ARINVOICE_V2` | 436K | **CDC** | `_operation_type != 'd'` |
| `stg_oda__arinvoicedetail` | `ODA_ARINVOICEDETAIL` | 611 | **CDC** | `_operation_type != 'd'` |

### AR Batch (7 models)

| Model | Source Table | Rows | Connector | Notes |
|-------|-------------|------|-----------|-------|
| `stg_oda__arinvoicepayment` | `ODA_ARINVOICEPAYMENT` | 37.6K | Batch | Payment headers |
| `stg_oda__arinvoicepaymentdetail` | `ODA_ARINVOICEPAYMENTDETAIL` | 172K | Batch | Payment line items |
| `stg_oda__arinvoicenetteddetail` | `ODA_ARINVOICENETTEDDETAIL` | 531K | Batch | Revenue netting |
| `stg_oda__arinvoiceadjustment` | `ODA_BATCH_ODA_ARINVOICEADJUSTMENT` | 10.7K | Batch | Adj headers |
| `stg_oda__arinvoiceadjustmentdetail` | `ODA_BATCH_ODA_ARINVOICEADJUSTMENTDETAIL` | 41K | Batch | Adj lines |
| `stg_oda__aradvance` | `ODA_BATCH_ODA_ARADVANCE` | 27 | Batch | AFE cash advances |
| `stg_oda__aradvancecloseout` | `ODA_BATCH_ODA_ARADVANCECLOSEOUT` | 0 | Batch | Schema only |

---

## Downstream Consumer Network

```
stg_oda__arinvoice_v2 ──────────┬──► int_oda_ar_invoice ──────► int_oda_ar_invoice_balances
                                 ├──► int_oda_ar_payments          int_oda_ar_invoice_adjustments_agg
                                 ├──► int_oda_ar_adjustments        int_oda_ar_invoice_netting_agg
                                 ├──► int_oda_ar_netting            int_oda_ar_invoice_payments_agg
                                 └──► dim_ar_summary ──────────► int_oda_ar_all_details

stg_oda__arinvoicedetail ───────► int_oda_ar_invoice

stg_oda__arinvoicepayment ──────► int_oda_ar_payments
stg_oda__arinvoicepaymentdetail ► int_oda_ar_payments

stg_oda__arinvoicenetteddetail ─► int_oda_ar_netting

stg_oda__arinvoiceadjustment ───► int_oda_ar_invoice
                                 └► int_oda_ar_adjustments
stg_oda__arinvoiceadjustmentdetail ► int_oda_ar_adjustments

stg_oda__aradvance ─────────────► int_oda_ar_advance_closeout_pairs
stg_oda__aradvancecloseout ─────► int_oda_ar_advance_closeout_pairs
```

---

## Column Rename Impact on Downstream Consumers

The following OLD column references in downstream models must be updated after refactoring:

### `stg_oda__arinvoice_v2` renames (affects 5+ consumers)

| Old Column | New Column | Affected Files |
|-----------|-----------|----------------|
| `Posted` (PascalCase) | `is_posted` | `int_oda_ar_invoice.sql`, `int_oda_ar_payments.sql`, `int_oda_ar_adjustments.sql`, `int_oda_ar_netting.sql`, `dim_ar_summary.sql` |
| `Code` (PascalCase) | `code` | `dim_ar_summary.sql` |
| `create_date` | `created_at` | `dim_ar_summary.sql` |
| `update_date` | `updated_at` | `dim_ar_summary.sql` |
| `include_in_accrual_report` | `is_include_in_accrual_report` | `dim_ar_summary.sql` |
| `fully_paid` | `is_fully_paid` | (check all consumers) |
| `print_pending` | `is_print_pending` | (check all consumers) |
| `flow_published_at` | `_flow_published_at` | (ingestion metadata prefix) |
| `flow_document` | **REMOVED** | Drop from all models |
| `_meta_op` | `_operation_type` | Filter consumers |

### `stg_oda__arinvoiceadjustmentdetail` renames

| Old Column | New Column | Affected Files |
|-----------|-----------|----------------|
| `Description` (PascalCase) | `description` | `int_oda_ar_adjustments.sql` |

### Filter condition updates (all intermediates)

Old: `where i.Posted = 1`
New: `where i.is_posted = true` (or `where i.is_posted`)

---

## Technical Approach

### CDC vs Batch Classification (Critical)

**DO NOT classify by `_meta/op` column presence** — Estuary puts it on ALL tables.

| Table | Connector Path | Connector Type | `_operation_type` Filter? |
|-------|---------------|---------------|--------------------------|
| `GL` | `FormenteraOps/ODA/oda/gl` | CDC | ✅ Required |
| `ODA_ARINVOICE_V2` | `FormenteraOps/ODA/oda/arinvoice_v2` | CDC | ✅ Required |
| `ODA_ARINVOICEDETAIL` | `FormenteraOps/ODA/oda/arinvoicedetail` | CDC | ✅ Required |
| `ODA_ARINVOICEPAYMENT` | `FormenteraOps/ODA_BATCH/oda/...` | Batch | ❌ Not needed |
| `ODA_ARINVOICENETTEDDETAIL` | `FormenteraOps/ODA_BATCH/oda/...` | Batch | ❌ Not needed |
| `ODA_BATCH_ODA_ARINVOICEADJUSTMENT` | `FormenteraOps/ODA_BATCH/oda_batch/...` | Batch | ❌ Not needed |
| etc. | | Batch | ❌ Not needed |

### 5-CTE Pattern Templates

**CDC variant** (for GL, arinvoice_v2, arinvoicedetail):
```sql
{{ config(
    materialized='view',
    tags=['oda', 'staging', 'formentera']
) }}

with source as (
    select * from {{ source('oda', 'TABLE_NAME') }}
),

renamed as (  -- noqa: ST06 (if needed for boolean conversions)
    select
        -- identifiers
        id::varchar                                    as id,
        owner_id::varchar                              as owner_id,
        -- dates
        created_at::timestamp_ntz                      as created_at,
        -- financial
        amount::decimal(18,2)                          as amount,
        -- flags
        coalesce(posted = 1, false)                    as is_posted,
        -- audit
        trim(created_by)::varchar                      as created_by,
        -- ingestion metadata
        "_meta/op"                                     as _operation_type,
        flow_published_at::timestamp_ntz               as _flow_published_at
    from source
),

filtered as (
    select * from renamed
    where _operation_type != 'd'
      and id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['id']) }} as {model}_sk,
        *,
        current_timestamp()                            as _loaded_at
    from filtered
),

final as (
    select
        -- identifiers
        {model}_sk,
        id,
        -- ... explicit column list ...
        -- ingestion metadata
        _operation_type,
        _flow_published_at,
        _loaded_at
    from enhanced
)

select * from final
```

**Batch variant** (for all other AR models):
```sql
-- Same as CDC but:
-- filtered CTE: WHERE id IS NOT NULL (no _operation_type check)
-- No _operation_type column exposed in output
```

### Financial Column Precision
- All monetary amounts: `::decimal(18,2)` (not `::float`)
- Rates and percentages: `::decimal(10,6)` if needed
- Integer flags that are booleans: `coalesce(COLUMN = 1, false)` → `is_{name}`

### Surrogate Key Naming
```
{model_name_without_stg_oda__}_sk
```

> ⚠️ **GL surrogate key exception**: Do NOT add a surrogate key to `stg_oda__gl`. The model
> is a view over 180M rows. `generate_surrogate_key` runs MD5 at query time on every consumer
> query — this adds per-row compute on the full 180M dataset for every `int_gl_enhanced` run.
> The GL enhanced CTE should contain only `current_timestamp() as _loaded_at`. Downstream
> joins use the natural key `id` (aliased as `gl_id`). Verify: `grep -r "gl_sk" models/` → 0 results.

| Model | Surrogate Key Column | Key Columns | Notes |
|-------|---------------------|-------------|-------|
| `stg_oda__gl` | **NONE** | N/A | 180M row view — SK overhead unacceptable |
| `stg_oda__arinvoice_v2` | `arinvoice_v2_sk` | `['id']` | |
| `stg_oda__arinvoicedetail` | `arinvoicedetail_sk` | `['id']` | |
| `stg_oda__arinvoicepayment` | `arinvoicepayment_sk` | `['id']` | |
| `stg_oda__arinvoicepaymentdetail` | `arinvoicepaymentdetail_sk` | `['id']` | |
| `stg_oda__arinvoicenetteddetail` | `arinvoicenetteddetail_sk` | `['id']` | |
| `stg_oda__arinvoiceadjustment` | `arinvoiceadjustment_sk` | `['id']` | |
| `stg_oda__arinvoiceadjustmentdetail` | `arinvoiceadjustmentdetail_sk` | `['id']` | |
| `stg_oda__aradvance` | `aradvance_sk` | `['id']` | |
| `stg_oda__aradvancecloseout` | `aradvancecloseout_sk` | `['id']` | 0 rows — schema only |

---

## Implementation Phases

### Phase 0: Setup + Schema Validation

**Tasks:**
1. Create branch from main: `git checkout main && git pull && git checkout -b feature/oda-staging-refactor-sprint-5-ar`
2. For EACH of the 10 source tables, run:
   ```sql
   select column_name, data_type, character_maximum_length, numeric_precision, numeric_scale
   from information_schema.columns
   where table_schema = 'ESTUARY_SCHEMA'
     and table_name in ('GL', 'ODA_ARINVOICE_V2', 'ODA_ARINVOICEDETAIL',
                        'ODA_ARINVOICEPAYMENT', 'ODA_ARINVOICEPAYMENTDETAIL',
                        'ODA_ARINVOICENETTEDDETAIL', 'ODA_BATCH_ODA_ARINVOICEADJUSTMENT',
                        'ODA_BATCH_ODA_ARINVOICEADJUSTMENTDETAIL',
                        'ODA_BATCH_ODA_ARADVANCE', 'ODA_BATCH_ODA_ARADVANCECLOSEOUT')
   order by table_name, ordinal_position;
   ```
3. Compare `information_schema` results against context table YAMLs — flag any type discrepancies
4. Check for Snowflake reserved word columns (`DATE`, `MONTH`, `YEAR`, `DAY`, `WEEK`, `QUARTER`)

**Files:**
- `context/sources/oda/tables/oda_arinvoice_v2.yaml` — reference for column definitions
- `context/sources/oda/tables/oda_arinvoicedetail.yaml`
- `context/sources/oda/tables/oda_arinvoicepayment.yaml`
- `context/sources/oda/tables/oda_arinvoicepaymentdetail.yaml`
- `context/sources/oda/tables/oda_arinvoicenetteddetail.yaml`
- `context/sources/oda/tables/oda_batch_oda_arinvoiceadjustment.yaml`
- `context/sources/oda/tables/oda_batch_oda_arinvoiceadjustmentdetail.yaml`
- `context/sources/oda/tables/oda_batch_oda_aradvance.yaml`
- `context/sources/oda/tables/oda_batch_oda_aradvancecloseout.yaml`

**Acceptance:** `information_schema` validated for all 10 tables before writing any SQL.

### Research Insights — Phase 0

**CDC Baseline (Mandatory before writing any CDC model):**
```sql
-- Record exact soft-deleted counts BEFORE refactoring.
-- These numbers go in the PR description to prove the post-deploy row count
-- drop is expected (not data loss).
SELECT
    "_meta/op"                   AS operation_type,
    COUNT(*)                     AS row_count,
    COUNT(CASE WHEN POSTED = 1 THEN 1 END) AS posted_count
FROM ESTUARY_DB.ESTUARY_SCHEMA.ODA_ARINVOICE_V2
GROUP BY 1 ORDER BY 1;

-- Expected AFTER count (what stg_oda__arinvoice_v2 will return post-deploy)
SELECT COUNT(*) AS expected_after_count
FROM ESTUARY_DB.ESTUARY_SCHEMA.ODA_ARINVOICE_V2
WHERE "_meta/op" != 'd';
```

**GL Uniqueness Check (Before adding surrogate key to anything):**
```sql
-- Verify ID is already unique post-filter (it should be — Estuary generates it)
SELECT COUNT(*) AS total_rows, COUNT(DISTINCT ID) AS distinct_ids
FROM ESTUARY_DB.ESTUARY_SCHEMA.GL
WHERE "_meta/op" != 'd';
-- Expected: total_rows == distinct_ids
```

**Decimal Precision Matrix:**
| Column Type | Cast | Example |
|-------------|------|---------|
| Dollar amounts | `decimal(18,2)` | `invoice_amount`, `payment_amount` |
| Volume (BOE, MCF) | `decimal(19,4)` | `net_volume_working`, `net_volume_non_working` |
| Interest rates | `decimal(10,6)` | `interest_rate`, `working_interest` |
| Boolean integers | `coalesce(COL = 1, false)` | `POSTED`, `FULLYPAID` |

Check `information_schema.columns` for numeric_scale to determine which category each column falls into.

---

### Phase 1: Create Directory + GL Refactor

**Tasks:**
1. Create `models/operations/staging/oda/accounts_receivable/` directory
2. Fully refactor `stg_oda__gl.sql` to 5-CTE pattern:
   - Fix tags: `['oda', 'staging', 'formentera']` (remove `'gl'`)
   - Add `filtered` CTE with `_operation_type != 'd' and id is not null`
   - Add `enhanced` CTE with **only** `current_timestamp() as _loaded_at` (NO surrogate key — see note)
   - Add `final` CTE with explicit column list
   - Remove `_loaded_at` from renamed CTE
   - Fix column casts: `::timestamp_ntz` instead of `CAST(... as TIMESTAMP)`
   - Add `trim()` on string columns
   - Move description CASE normalization from `renamed` CTE to `enhanced` CTE
     (convention: `renamed` = type casts only; business defaults belong in `enhanced`)
3. Move `stg_oda__gl.sql` from `oda/` to `general_ledger/` using `git mv`
4. Add GL source entry to `general_ledger/_src_oda__general_ledger.yml`
   (preserve existing `dbt_utils.unique_combination_of_columns` test on `ID + "_meta/op"`)
5. Remove GL entry from `src_oda.yml`
6. Add GL column docs to `general_ledger/_stg_oda__general_ledger.yml`
7. Remove `stg_oda__gl` model definition from `schema.yml` root and add it to
   `general_ledger/_stg_oda__general_ledger.yml`
8. Fix GL FK test severity in `_stg_oda__general_ledger.yml`:
   - `company_id → stg_oda__company_v2.id`: add `config: severity: warn`
   - `afe_id → stg_oda__afe_v2.id`: add `config: severity: warn` + `config: where: "afe_id is not null"`
   - `source_module` accepted_values: add `config: severity: warn` (enumeration is incomplete)

> ⚠️ **No surrogate key on GL**: `generate_surrogate_key` runs MD5 at query time on 180M rows
> for every consumer query (including `int_gl_enhanced` which scans GL 3 times per incremental run).
> The natural key `id` is already stable and unique post-filter. Downstream uses `id` as `gl_id`.
> Verify before starting: `grep -r "gl_sk" models/` — expect 0 results.

**Files created/modified:**
- `models/operations/staging/oda/general_ledger/stg_oda__gl.sql` (moved + refactored)
- `models/operations/staging/oda/general_ledger/_src_oda__general_ledger.yml` (add GL source)
- `models/operations/staging/oda/general_ledger/_stg_oda__general_ledger.yml` (add GL docs, model def)
- `models/operations/staging/oda/src_oda.yml` (remove GL entry)
- `models/operations/staging/oda/schema.yml` (remove stg_oda__gl model definition)

**Validation:**
```bash
python scripts/validate_staging.py models/operations/staging/oda/general_ledger/stg_oda__gl.sql
dbt build --select stg_oda__gl
dbt show --select stg_oda__gl --limit 5
```

---

### Phase 2: AR CDC Models (stg_oda__arinvoice_v2 + stg_oda__arinvoicedetail)

These are the highest-priority AR models — CDC tables leaking soft deletes.

> ⚠️ **CDC baseline required before starting this phase.** Run the baseline queries from
> Phase 0 and paste the row counts in the PR description BEFORE writing any SQL.

**Tasks (arinvoice_v2):**
1. Verify information_schema types for all columns
2. Refactor to 5-CTE CDC variant
3. Key renames from current model:
   - `POSTED` → `coalesce(POSTED = 1, false) as is_posted`
   - `FULLYPAID` → `coalesce(FULLYPAID = 1, false) as is_fully_paid` (verify int vs bool in IS)
   - `INCLUDEINACCRUALREPORT` → `coalesce(INCLUDEINACCRUALREPORT = 1, false) as is_include_in_accrual_report`
   - `PRINTPENDING` → `coalesce(PRINTPENDING = 1, false) as is_print_pending`
   - `ISOVERAGEINVOICE` → `ISOVERAGEINVOICE::boolean as is_overage_invoice` (check IS type)
   - `CREATEDATE` → `CREATEDATE::timestamp_ntz as created_at`
   - `UPDATEDATE` → `UPDATEDATE::timestamp_ntz as updated_at`
   - `RECORDINSERTDATE` → `RECORDINSERTDATE::timestamp_ntz as record_inserted_at`
   - `RECORDUPDATEDATE` → `RECORDUPDATEDATE::timestamp_ntz as record_updated_at`
   - `FLOW_PUBLISHED_AT` → `FLOW_PUBLISHED_AT::timestamp_ntz as _flow_published_at` (underscore prefix)
   - `"_meta/op"` → `"_meta/op" as _operation_type`
   - `INVOICEAMOUNT` → `INVOICEAMOUNT::decimal(18,2) as invoice_amount`
   - `FLOW_DOCUMENT` → **DROP** (do not include)
4. Add to `accounts_receivable/` directory
5. Create `_src_oda__accounts_receivable.yml` with CDC source entry

**Tasks (arinvoicedetail):**
1. Verify information_schema types
2. Refactor to 5-CTE CDC variant (similar pattern to arinvoice_v2)
3. Key consideration: only 611 rows — CDC filter still required

**Files created:**
- `models/operations/staging/oda/accounts_receivable/stg_oda__arinvoice_v2.sql`
- `models/operations/staging/oda/accounts_receivable/stg_oda__arinvoicedetail.sql`
- `models/operations/staging/oda/accounts_receivable/_src_oda__accounts_receivable.yml` (stub — full docs in Phase 5)

**Validation:**
```bash
python scripts/validate_staging.py models/operations/staging/oda/accounts_receivable/stg_oda__arinvoice_v2.sql
python scripts/validate_staging.py models/operations/staging/oda/accounts_receivable/stg_oda__arinvoicedetail.sql
dbt show --select stg_oda__arinvoice_v2 --limit 10
dbt show --select stg_oda__arinvoicedetail --limit 10
```

---

### Phase 3: AR Batch Models (7 models)

**Tasks for each batch model:**
1. Verify information_schema types
2. Add config block: `materialized='view'`, `tags=['oda', 'staging', 'formentera']`
3. Refactor to 5-CTE batch variant
4. Apply standard column renames (camelCase → snake_case, boolean `is_` prefix)
5. Cast financial amounts as `decimal(18,2)`
6. Move to `accounts_receivable/` directory
7. Add source entry to `_src_oda__accounts_receivable.yml`

**Model-specific notes:**

| Model | Notes |
|-------|-------|
| `stg_oda__arinvoicepayment` | Check `POSTED` → `is_posted` boolean type |
| `stg_oda__arinvoicepaymentdetail` | `_meta_op` commented out in current model — omit from output |
| `stg_oda__arinvoicenetteddetail` | 531K rows — `net_volume_*` columns use `decimal(19,4)` not `decimal(18,2)` |
| `stg_oda__arinvoiceadjustment` | `INCLUDEINACCRUALREPORT`, `POSTED` booleans |
| `stg_oda__arinvoiceadjustmentdetail` | Check `FLOW_DOCUMENT` — drop |
| `stg_oda__aradvance` | Only 27 rows; `is_after_casing` already has `is_` prefix; `expense_deck_interest_total` may need `decimal(10,6)` — check IS |
| `stg_oda__aradvancecloseout` | 0 rows — schema only; add `not_null` tests on both voucher FKs for when rows arrive |

### Research Insights — arinvoicenetteddetail Decimal Types

This model has a mix of dollar amounts and volume measurements. The data integrity
guardian and performance oracle both flag this:
- `netted_amount`, payment-related amounts → `decimal(18,2)`
- `net_volume_non_working`, `net_volume_working` → `decimal(19,4)` (fractional BOE/MCF units)
- Verify each against `information_schema.columns` — specifically `numeric_scale`

### Research Insights — NULL Financial Values

Financial NULL safety pattern (matches existing `stg_oda__gl.sql` convention):
```sql
-- In renamed CTE — null-coalesce monetary amounts
coalesce(INVOICEAMOUNT, 0)::decimal(18,2)  as invoice_amount,
-- Same for all financial columns — prevents NULL propagation into SUM() aggregations
```

### Research Insights — Inner-Join Anti-Pattern Check

Before finalizing the downstream consumer updates (Phase 6), check AR intermediates for
the inner-join-after-left-join anti-pattern. If a FK can be NULL:
1. The staging YAML will have `severity: warn` on the `not_null` test
2. Every downstream join on that column must be `left join`, not `inner join`
3. A chain like `left join → inner join → SUM()` silently drops rows

Columns most at risk: `well_id`, `project_id`, `afe_id` on `stg_oda__arinvoice_v2`.

**Files created:**
- `models/operations/staging/oda/accounts_receivable/stg_oda__arinvoicepayment.sql`
- `models/operations/staging/oda/accounts_receivable/stg_oda__arinvoicepaymentdetail.sql`
- `models/operations/staging/oda/accounts_receivable/stg_oda__arinvoicenetteddetail.sql`
- `models/operations/staging/oda/accounts_receivable/stg_oda__arinvoiceadjustment.sql`
- `models/operations/staging/oda/accounts_receivable/stg_oda__arinvoiceadjustmentdetail.sql`
- `models/operations/staging/oda/accounts_receivable/stg_oda__aradvance.sql`
- `models/operations/staging/oda/accounts_receivable/stg_oda__aradvancecloseout.sql`

**Validation:**
```bash
python scripts/validate_staging.py models/operations/staging/oda/accounts_receivable/
dbt build --select stg_oda__arinvoicepayment stg_oda__arinvoicepaymentdetail stg_oda__arinvoicenetteddetail stg_oda__arinvoiceadjustment stg_oda__arinvoiceadjustmentdetail stg_oda__aradvance stg_oda__aradvancecloseout
```

---

### Phase 4: YAML Documentation + Tests

Create comprehensive `_stg_oda__accounts_receivable.yml` with full column docs for all 9 AR models.

**Structure per model:**
```yaml
- name: stg_oda__arinvoice_v2
  description: |
    Staging model for ODA Accounts Receivable invoice headers.
    Source: ODA_ARINVOICE_V2 (Estuary CDC, 436K rows)
    Grain: One row per AR invoice (id)
    Note: Soft deletes filtered via _operation_type != 'd'
  columns:
    - name: arinvoice_v2_sk
      description: Surrogate key derived from id
    - name: id
      description: Primary key — GUID
      data_tests:
        - unique
        - not_null
    - name: is_posted
      description: Whether the invoice has been posted to the GL
    # ... (all columns)
    - name: _loaded_at
      description: Timestamp when record was loaded by dbt
    - name: _flow_published_at
      description: Estuary publication timestamp
    - name: _operation_type
      description: CDC operation type — 'c' create, 'd' delete (all 'd' filtered out)
```

**FK tests using `arguments:` wrapper:**
```yaml
- relationships:
    arguments:
      to: ref('stg_oda__company_v2')
      field: id
    config:
      severity: warn
```

**Notes on FK test decisions:**
- Test `company_id → stg_oda__company_v2.id` (`severity: warn` — some orphans expected)
- Test `owner_id → stg_oda__owner_v2.id` (`severity: warn`)
- Test `well_id → stg_oda__wells.id` (`severity: warn`)
- `arinvoiceadjustmentdetail.afe_id` → references `ODA_AFE` which is NOT synced by Estuary.
  Remove `relationships` test entirely. Keep `not_null: config: severity: warn`.
  Document in YAML: "AFEID references ODA_AFE which is not synced by Estuary; FK test removed."
- `aradvance.afe_id` — same treatment as above
- `arinvoicepaymentdetail.invoice_payment_id → stg_oda__arinvoicepayment.id`: safe to test
  at `severity: error` (both from same batch connector)
- `arinvoicepaymentdetail.invoice_id → stg_oda__arinvoice_v2.id`: use `severity: warn`
  (crosses connector types — detail is batch, invoice is CDC; timing gap possible)
- For `aradvancecloseout` (0 rows): add `not_null: config: severity: warn` on BOTH `voucher_id`
  and `target_voucher_id`. These are the FK columns the advance closeout pairs intermediate needs.
  Flag in YAML: "Join direction in int_oda_ar_advance_closeout_pairs may be inverted — verify
  when first rows arrive."

### Research Insights — ODA FK Column Naming Convention

Per `docs/solutions/refactoring/oda-context-fk-column-naming-pattern.md`:
ODA drops the entity prefix on child FK columns. The parent table name disambiguates,
not the column name. Example: `ODA_ARINVOICE_V2` has FK column `INVOICEID` (not
`ARINVOICEID`). Always verify against context table YAMLs in
`context/sources/oda/tables/` before writing YAML relationship tests.

**Files created:**
- `models/operations/staging/oda/accounts_receivable/_stg_oda__accounts_receivable.yml`
- Update `models/operations/staging/oda/accounts_receivable/_src_oda__accounts_receivable.yml` with full source docs

---

### Phase 5: Source YAML Cleanup → src_oda.yml Empty

**Tasks:**
1. Move all AR source entries from `src_oda.yml` to `accounts_receivable/_src_oda__accounts_receivable.yml`
2. Ensure GL source entry has been moved to `general_ledger/_src_oda__general_ledger.yml` (done in Phase 1)
3. Remove the last remaining entries from `src_oda.yml`
4. After removal, `src_oda.yml` should look like:
   ```yaml
   version: 2

   sources:
     - name: oda
       database: ESTUARY_DB
       schema: ESTUARY_SCHEMA
       tables: []
   ```
5. OR — verify if it's safe to delete `src_oda.yml` entirely. Check if any other models reference `source('oda', ...)` outside of the domain YAML files.

**Also clean up:**
- `models/operations/staging/oda/schema.yml` — check if this is empty/stale (it likely has old model docs). Remove any AR/GL entries that have been moved to domain YAMLs.

**Validation:**
```bash
# Verify no models still reference src_oda.yml source declarations
grep -r "source('oda'" models/ | grep -v "_src_oda__" | grep -v "schema.yml"
dbt parse --warn-error --no-partial-parse
```

---

### Phase 6: Downstream Consumer Updates

Update all 11 downstream files that reference old AR staging column names.

**Priority order (fix most-broken first):**

1. **`int_oda_ar_invoice.sql`** — Filter `i.Posted = 1` → `i.is_posted = true`
2. **`int_oda_ar_payments.sql`** — Filter `i.Posted = 1` → `i.is_posted = true`
3. **`int_oda_ar_adjustments.sql`** — Filter `i.Posted = 1` → `i.is_posted = true`; `ariad.Description` → `ariad.description`
4. **`int_oda_ar_netting.sql`** — Filter `i.Posted = 1` → `i.is_posted = true`
5. **`dim_ar_summary.sql`** — Major updates:
   - `i.Posted` → `i.is_posted`
   - `i.Code` → `i.code`
   - `i.create_date` → `i.created_at`
   - `i.update_date` → `i.updated_at`
   - `i.include_in_accrual_report` → `i.is_include_in_accrual_report`
6. **`int_oda_ar_invoice_payments_agg.sql`** — Also fix the pre-existing SQL syntax error: trailing comma before `FROM`
7. **`int_oda_ar_invoice_adjustments_agg.sql`** — Verify no column renames needed
8. **`int_oda_ar_invoice_netting_agg.sql`** — Verify no column renames needed
9. **`int_oda_ar_invoice_balances.sql`** — Verify no column renames needed
10. **`int_oda_ar_all_details.sql`** — Verify no column renames needed
11. **`int_oda_ar_advance_closeout_pairs.sql`** — Verify no column renames needed

**Process for each file:**
1. Read the current model
2. Identify all references to old staging column names
3. Update to new names
4. Run `dbt build --select {model_name}` to verify

**Validation:**
```bash
dbt build --select +stg_oda__arinvoice_v2+ --exclude stg_oda__arinvoice_v2
dbt build --select +stg_oda__aradvancecloseout+ --exclude stg_oda__aradvancecloseout
```

---

### Phase 6b: Pre-Deploy Verification (Run Before Requesting Review)

**Go/No-Go gates (ALL must pass before PR is approved for merge):**

| Gate | Check | Command |
|------|-------|---------|
| Structural lint | All 10 models pass validate_staging.py | `python scripts/validate_staging.py ...` |
| YAML parse | No warnings or errors | `dbt parse --warn-error --no-partial-parse` |
| No `Posted = 1` | Old integer filter removed from all intermediates | `grep -r "\.Posted\s*=" models/` → 0 results |
| No `create_date` | Old audit column removed from all consumers | `grep -r "create_date" models/operations` → 0 results |
| No `flow_document` | Dropped from all staging models | `grep -r "flow_document" models/operations/staging/oda/accounts_receivable/` → 0 results |
| Source declarations | All 10 tables in domain YAMLs before src_oda.yml cleanup | `dbt parse` after YAML split |
| CDC baseline documented | Row count delta in PR description | Manual — paste numbers |
| No `gl_sk` | Surrogate key not in GL model or downstream | `grep -r "gl_sk" models/` → 0 results |

**Post-deploy spot check (run within 5 minutes of merge):**
```sql
-- Verify CDC filter applied correctly (row count must match pre-deploy baseline)
SELECT COUNT(*) FROM FO_DEV_DB.STAGING.STG_ODA__ARINVOICE_V2;

-- Verify boolean cast (must return only TRUE/FALSE, no NULLs)
SELECT is_posted, COUNT(*) FROM FO_DEV_DB.STAGING.STG_ODA__ARINVOICE_V2 GROUP BY 1;

-- Verify no CDC 'd' records leaked through
SELECT _operation_type, COUNT(*) FROM FO_DEV_DB.STAGING.STG_ODA__ARINVOICE_V2 GROUP BY 1;
-- Must NOT contain 'd'
```

---

### Phase 7: Final Validation + PR

**Validation checklist:**
```bash
# 1. Structural validation (all 10 models)
python scripts/validate_staging.py models/operations/staging/oda/accounts_receivable/
python scripts/validate_staging.py models/operations/staging/oda/general_ledger/stg_oda__gl.sql

# 2. dbt parse (catch YAML syntax errors)
dbt parse --warn-error --no-partial-parse

# 3. Build all new models
dbt build --select stg_oda__arinvoice_v2 stg_oda__arinvoicedetail stg_oda__arinvoicepayment stg_oda__arinvoicepaymentdetail stg_oda__arinvoicenetteddetail stg_oda__arinvoiceadjustment stg_oda__arinvoiceadjustmentdetail stg_oda__aradvance stg_oda__aradvancecloseout stg_oda__gl

# 4. Build downstream consumers
dbt build --select +stg_oda__arinvoice_v2+ --exclude stg_oda__arinvoice_v2

# 5. Data preview
dbt show --select stg_oda__arinvoice_v2 --limit 10
dbt show --select stg_oda__gl --limit 10

# 6. Lint
sqlfluff lint models/operations/staging/oda/accounts_receivable/ models/operations/staging/oda/general_ledger/stg_oda__gl.sql
```

**PR requirements:**
- Branch: `feature/oda-staging-refactor-sprint-5-ar`
- Title: `refactor(oda): Sprint 5 — 9 AR staging models + GL to 5-CTE`
- Link Linear issues: FOR-280, FOR-281, FOR-282
- Body should mention: 46/46 models complete, src_oda.yml now empty

---

## Acceptance Criteria

- [ ] `stg_oda__gl` fully refactored (5-CTE, correct tags, surrogate key) and moved to `general_ledger/`
- [ ] 2 AR CDC models have `_operation_type != 'd'` in `filtered` CTE
- [ ] 7 AR batch models have `id is not null` in `filtered` CTE (no CDC filter)
- [ ] All 10 models have `materialized='view'` and `tags=['oda', 'staging', 'formentera']`
- [ ] Surrogate key + `_loaded_at` in `enhanced` CTE for all 10 models
- [ ] Explicit column list in `final` CTE (no `SELECT *`)
- [ ] All 10 models in correct domain directories
- [ ] Financial amounts cast as `decimal(18,2)` not `float`
- [ ] Boolean columns with `is_` prefix (`is_posted`, `is_fully_paid`, etc.)
- [ ] `FLOW_DOCUMENT` dropped from all models
- [ ] Source entries moved from `src_oda.yml` to domain YAML files
- [ ] `src_oda.yml` is empty after this sprint (all 46 tables in domain directories)
- [ ] `_stg_oda__accounts_receivable.yml` with full column docs for all 9 AR models
- [ ] All downstream consumers updated — zero broken refs
- [ ] `python scripts/validate_staging.py` passes on all 10 models
- [ ] `dbt parse --warn-error --no-partial-parse` passes
- [ ] `dbt build` passes with 0 errors on all 10 models + downstream consumers
- [ ] `-- noqa: ST06` on renamed CTEs with boolean conversions (if needed)
- [ ] `-- noqa: RF06` on any Snowflake reserved word columns
- [ ] PR created on `feature/oda-staging-refactor-sprint-5-ar`, linked to FOR-280/FOR-281/FOR-282
- [ ] **Completes the ODA Staging Refactor project (46/46 models)**

---

## Key Gotchas (Do Not Skip)

1. **stg_oda__gl.sql is NOT already refactored** — it's 2-CTE with wrong tags. Full refactor required.
2. **`_meta/op` on all Estuary tables** — classify by connector config path, not column presence.
3. **`arguments:` wrapper** required on `relationships` AND `accepted_values` tests.
4. **ALWAYS check `information_schema.columns`** — context docs sometimes differ from actual Snowflake types.
5. **Snowflake reserved words** (`DATE`, `MONTH`, etc.) must be double-quoted with `-- noqa: RF06`.
6. **Unsynchronized FK pattern** — if a relationships test shows 100% failure, use `severity: warn`.
7. **Write files sequentially** — swarm agents must NOT write multiple files in parallel (causes cascading failures).
8. **`src_oda.yml` has OLD entries** (`ODA_BATCH_ODA_ENTITY`, `ODA_BATCH_ODA_COMPANY`, etc.) — these are not in scope for Sprint 5 but are documented for reference.
9. **NO surrogate key on `stg_oda__gl`** — MD5 at query time on 180M rows is unacceptable overhead. `enhanced` CTE contains only `_loaded_at`.
10. **`int_oda_ar_invoice_payments_agg.sql`** has a pre-existing SQL syntax error (trailing comma) — fix it.
11. **`stg_oda__aradvancecloseout`** has 0 rows — schema-only table. Still must pass validator.
12. **`is_overage_invoice`** on `stg_oda__arinvoice_v2` — check actual type in IS (may be native boolean vs integer).
13. **Volume vs dollar decimal types** — `net_volume_*` columns in `arinvoicenetteddetail` use `decimal(19,4)`. Dollar amounts use `decimal(18,2)`. Check `numeric_scale` in `information_schema`.
14. **GL description CASE logic** belongs in `enhanced` CTE, not `renamed`. Move it during the refactor.
15. **`schema.yml` GL model definition** must be removed from the root `schema.yml` and added to `general_ledger/_stg_oda__general_ledger.yml`.
16. **4 intermediate models filter `i.Posted = 1`** — this is a compile-success, runtime-failure. ALL four must be updated to `i.is_posted = true` BEFORE merge.
17. **`-- noqa: ST06`** required on `renamed` CTEs that mix simple renames with `coalesce()` boolean conversions. Add to the `select` keyword, not the individual column.
18. **CI defer stale column names** — after AR renames merge, any subsequent PR touching AR downstream consumers may fail CI until the next prod dbt run. Warn the team.
19. **ODA FK column naming drops entity prefix** — `INVOICEID` not `ARINVOICEID`. Always verify against context table YAMLs before writing relationship tests.
20. **NULL financial values** — `coalesce(AMOUNT, 0)::decimal(18,2)` prevents NULL propagation into downstream SUM() aggregations. Match the existing `stg_oda__gl` convention.
21. **`aradvancecloseout` voucher join direction** may be inverted in `int_oda_ar_advance_closeout_pairs`. Flag in YAML for verification when rows arrive.

---

## References

### Internal References
- **5-CTE CDC reference**: `models/operations/staging/oda/accounts_payable/stg_oda__apinvoice.sql`
- **5-CTE batch reference**: `models/operations/staging/oda/afe_budgeting/stg_oda__afebudget.sql`
- **YAML reference**: `models/operations/staging/oda/afe_budgeting/_stg_oda__afe_budgeting.yml`
- **Source YAML reference**: `models/operations/staging/oda/general_ledger/_src_oda__general_ledger.yml`
- **Staging conventions**: `docs/conventions/staging.md`
- **Context docs**: `context/sources/oda/domains/accounts_receivable.yaml`
- **Table context**: `context/sources/oda/tables/oda_arinvoice_v2.yaml` (and others)
- **Brainstorm**: `docs/brainstorms/2026-02-17-oda-staging-refactor-brainstorm.md`

### Solutions to Consult
- `docs/solutions/refactoring/oda-context-fk-column-naming-pattern.md` — FK column prefix dropping
- `docs/solutions/build-errors/snowflake-reserved-word-cast-failure.md` — reserved word quoting
- `docs/solutions/refactoring/oda-context-documentation-sprint-0.md` — CDC vs batch methodology

### Related PRs
- PR #270 (Sprint 1 — AP + JIB)
- PR #274 (Sprint 2 — Supporting + Decks)
- PR #275 (Sprint 3 — GL Lookups + Master Data)
- PR #276 (Sprint 3b — AFE/Budgeting)

---

## Tech Debt (Out of Scope for Sprint 5 — File as Follow-Up Issues)

These were discovered during research but are not blockers for this sprint:

| Issue | File | Priority |
|-------|------|---------|
| `int_oda_gl.sql` is a view scanning 180M GL rows twice via UNION ALL | `models/operations/intermediate/finance/int_oda_gl.sql` | High |
| `int_oda_gl.sql` uses `GL.IS_POSTED = 1` (integer compare on boolean) | lines 64, 132 | Medium |
| `int_gl_enhanced` vs `int_general_ledger_enhanced` — divergent watermarks | `_loaded_at` vs `created_at`/`updated_at` | Medium |
| `int_oda_ar_advance_closeout_pairs` — verify voucher join direction when rows arrive | downstream of `stg_oda__aradvancecloseout` | Low |
| `stg_oda__revenue_deck_participant` (196M rows, surrogate key) — audit downstream consumers | `models/operations/staging/oda/decks/` | Medium |
