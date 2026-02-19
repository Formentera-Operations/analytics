---
module: ODA Staging
date: 2026-02-18
problem_type: best_practice
component: database
symptoms:
  - "Boolean columns assumed to be integer flags (0/1) but are actually BOOLEAN type"
  - "Surrogate key added to 180M-row view causing MD5 overhead on every consumer query"
  - "Batch table has _meta/op column but should not have CDC filter applied"
  - "aradvancecloseout (0 rows) needs staging model for future data"
root_cause: inadequate_documentation
resolution_type: documentation_update
severity: medium
tags: [oda, ar, accounts-receivable, staging, 5-cte, boolean, surrogate-key, large-view, cdc-batch, sprint-5]
---

# ODA Staging Refactor Sprint 5 — AR + GL Patterns

Patterns and gotchas from the ODA staging refactor Sprint 5 (9 AR models + GL). Sprint 5
completed the 46/46 model refactor. This doc captures decisions specific to the AR domain
and GL model that differ from prior sprints.

## Key Decisions & Patterns

### 1. ODA AR Boolean Columns Are Native BOOLEAN (Not Integer Flags)

**Expected:** ODA integer flags (0/1) requiring `coalesce(POSTED = 1, false)` cast
**Actual:** All ODA AR boolean columns are native Snowflake `BOOLEAN` type

```sql
-- ❌ WRONG — Integer cast pattern (AP invoices use this, AR does not)
coalesce(POSTED = 1, false)             as is_posted,
coalesce(FULLYPAID = 1, false)          as is_fully_paid,

-- ✅ CORRECT — Native boolean cast (AR tables)
POSTED::boolean                         as is_posted,
FULLYPAID::boolean                      as is_fully_paid,
INCLUDEINACCRUALREPORT::boolean         as is_include_in_accrual_report,
ISOVERAGEINVOICE::boolean               as is_overage_invoice,
PRINTPENDING::boolean                   as is_print_pending,
```

**Tables confirmed as native BOOLEAN:**
`ODA_ARINVOICE_V2`, `ODA_ARINVOICEPAYMENT`, `ODA_BATCH_ODA_ARINVOICEADJUSTMENT`,
`ODA_BATCH_ODA_ARADVANCE`

**Rule:** Always run `information_schema.columns` query to verify `data_type` before
choosing between `::boolean` and `coalesce(X = 1, false)`.

---

### 2. No Surrogate Key on stg_oda__gl (180M Row View)

**Decision:** `stg_oda__gl` intentionally has NO surrogate key.

**Reason:** `generate_surrogate_key()` compiles to an MD5 hash over concatenated column
values. As a VIEW (not a table), this hash runs at query time on EVERY consumer query.
At 180M rows with multiple downstream consumers (int_gl_enhanced scans the view 3 times
per incremental run), this adds unacceptable per-query compute overhead.

```sql
-- ❌ WRONG — Adds MD5 overhead on every query against this 180M row view
enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['id']) }} as gl_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- ✅ CORRECT — _loaded_at only; downstream joins use natural key `id` (aliased as gl_id)
enhanced as (
    select
        *,
        current_timestamp() as _loaded_at
    from filtered
),
```

**How to verify it's safe:** `grep -r "gl_sk" models/` → 0 results. No downstream
model currently joins on a `gl_sk`. They all join on `id` (aliased as `gl_id`).

**Alternative:** If a surrogate key is truly required downstream, add `generate_surrogate_key`
inside the incremental intermediate model (`int_gl_enhanced`) where it runs only on new/changed
rows during each incremental batch — not on all 180M rows on every query.

**Threshold guidance:** Consider no surrogate key for staging views when:
- View has > 50M rows, AND
- Multiple consumer models scan it, AND
- The natural key (`id`) is already stable and unique post-CDC-filter

---

### 3. aradvancecloseout Has `_meta/op` Column But Is a Batch Table

**Symptom:** `ODA_BATCH_ODA_ARADVANCECLOSEOUT` has a `_meta/op` column in Snowflake
(confirmed via `information_schema.columns`), which looks like a CDC table indicator.

**Reality:** This is a BATCH table. Estuary puts `_meta/op` on ALL tables it syncs —
both CDC and batch. For batch tables, `_meta/op` always contains only `'c'` (never `'d'`).

**Rule (from Sprint 0):** Classify CDC vs batch by Estuary **connector path**, not by
`_meta/op` column presence:
- `FormenteraOps/ODA/oda/*` → CDC (requires `_operation_type != 'd'` filter)
- `FormenteraOps/ODA_BATCH/oda_*/...` → Batch (no filter needed, do NOT expose `_operation_type`)

```sql
-- ❌ WRONG — Adding CDC filter to batch table
filtered as (
    select * from renamed
    where _operation_type != 'd'  -- aradvancecloseout is NOT CDC!
      and id is not null
),

-- ✅ CORRECT — Batch table filtered by id only
filtered as (
    select * from renamed
    where id is not null
),
```

---

### 4. Zero-Row Schema-Only Table Still Requires Full Model

`stg_oda__aradvancecloseout` has 0 rows (schema-only). The downstream
`int_oda_ar_advance_closeout_pairs` depends on it. The model must still be built:

```sql
-- Build the 5-CTE model normally — it produces an empty result set, which is correct
-- The downstream intermediate produces zero rows (correct behavior) until data arrives
```

**When rows eventually arrive:** The `voucher_id`/`target_voucher_id` join direction in
`int_oda_ar_advance_closeout_pairs` must be verified. Context doc and intermediate use
opposite naming conventions — could be inverted. See the YAML description:
```yaml
- name: voucher_id
  description: >
    FK to the original advance voucher being closed out.
    NOTE: Join direction in int_oda_ar_advance_closeout_pairs must be verified
    when first rows arrive — context doc and intermediate use opposite naming.
```

---

### 5. Downstream Consumers Use Integer Boolean Filter Syntax

All existing AR intermediate models were filtering on `Posted = 1` (integer syntax)
because the old staging model exposed the raw integer value. After refactoring to
native boolean, all 4 filter references need updating:

```sql
-- ❌ OLD — Integer comparison (breaks after boolean refactor)
where i.Posted = 1

-- ✅ NEW — Native boolean (correct after refactor)
where i.is_posted
```

**Files requiring this fix:**
- `int_oda_ar_invoice.sql`
- `int_oda_ar_payments.sql`
- `int_oda_ar_adjustments.sql`
- `int_oda_ar_netting.sql`

**Also in dim_ar_summary:** PascalCase column aliases from the mart must be updated to
reference new snake_case names while preserving output aliases:
```sql
-- ❌ OLD
i.Posted as posted,
i.create_date as create_date,
i.update_date as update_date,
i.include_in_accrual_report

-- ✅ NEW (preserves output aliases for backward compat)
i.is_posted as posted,
i.created_at as create_date,
i.updated_at as update_date,
i.is_include_in_accrual_report as include_in_accrual_report
```

---

### 6. Volume vs Dollar Decimal Precision in AR

Not all FLOAT columns in AR models should use `decimal(18,2)`. Volume measurements
(BOE, MCF) need extra decimal places:

| Column Type | Cast | Example Columns |
|-------------|------|----------------|
| Dollar amounts | `::decimal(18,2)` | `invoice_amount`, `payment_amount`, `adjustment_amount` |
| Volume (BOE/MCF) | `::decimal(19,4)` | `net_volume_working`, `net_volume_non_working` in arinvoicenetteddetail |
| Integer volumes | `::int` | `net_volume` in arinvoiceadjustmentdetail (NUMBER scale 0 in IS) |
| Financial interest | `::decimal(18,2)` | `expense_deck_interest_total` in aradvance |

**Rule:** Always check `numeric_scale` in `information_schema.columns` to determine
the correct decimal precision. Do not assume all numeric columns are `decimal(18,2)`.

---

## Sprint 5 Build Validation Results

| Model | Source | Type | Build |
|-------|--------|------|-------|
| stg_oda__arinvoice_v2 | ODA_ARINVOICE_V2 | CDC | ✅ |
| stg_oda__arinvoicedetail | ODA_ARINVOICEDETAIL | CDC | ✅ |
| stg_oda__arinvoicepayment | ODA_ARINVOICEPAYMENT | Batch | ✅ |
| stg_oda__arinvoicepaymentdetail | ODA_ARINVOICEPAYMENTDETAIL | Batch | ✅ |
| stg_oda__arinvoicenetteddetail | ODA_ARINVOICENETTEDDETAIL | Batch | ✅ |
| stg_oda__arinvoiceadjustment | ODA_BATCH_ODA_ARINVOICEADJUSTMENT | Batch | ✅ |
| stg_oda__arinvoiceadjustmentdetail | ODA_BATCH_ODA_ARINVOICEADJUSTMENTDETAIL | Batch | ✅ |
| stg_oda__aradvance | ODA_BATCH_ODA_ARADVANCE | Batch | ✅ |
| stg_oda__aradvancecloseout | ODA_BATCH_ODA_ARADVANCECLOSEOUT | Batch | ✅ (0 rows) |
| stg_oda__gl | GL | CDC | ✅ (180M rows, view) |

`validate_staging.py`: 10 checked, 9 passed, 0 errors, 1 expected warning (no SK on GL)
`dbt parse --warn-error --no-partial-parse`: clean

## Related Issues

- See also: [oda-context-documentation-sprint-0.md](./oda-context-documentation-sprint-0.md) — CDC vs batch classification methodology
- See also: [dbt-incremental-column-rename-requires-full-refresh-20260218.md](../build-errors/dbt-incremental-column-rename-requires-full-refresh-20260218.md) — int_gl_enhanced full-refresh requirement
- See also: [drilling-mart-sprint-2-fact-table-patterns.md](./drilling-mart-sprint-2-fact-table-patterns.md) — surrogate key and materialization decisions
- PR #277: https://github.com/Formentera-Operations/analytics/pull/277
