# AR Aging Refactor — Intermediate Layer Redesign + New Fact Tables

**Date:** 2026-02-18
**Author:** Rob Stover
**Status:** Design Complete → Ready for Planning

---

## What We're Building

A redesigned AR aging pipeline that:

1. Removes the hard-coded posted filter from the invoice base model, enabling **pre-JIB forward-looking AR analysis** alongside the existing instantaneous AR aging balance view
2. Adds **posted/unposted balance split** throughout the aggregation layer
3. Extracts advance/closeout exclusion logic into a dedicated remaining balance model
4. Replaces the single `fct_ar_aging` with two purpose-built facts:
   - `fct_ar_aging_detail` — transaction-level (one row per transaction/invoice type)
   - `fct_ar_aging_summary` — invoice-level with standard AR aging bucket columns

---

## Business Motivation

- **Primary use case**: Formentera wants to view pending AR before running JIB — unposted invoices represent upcoming billing that treasury and operations need to anticipate
- **Secondary use case**: Standard instantaneous AR aging balance (what's currently posted and outstanding)
- **Consumption model**: Claude Desktop via Skills and Snowflake MCP — not a traditional BI dashboard. Schema clarity matters more than performance optimization.

---

## Key Decisions

### Decision 1: Unfiltered Invoice Base ✅

**Chosen approach:** Remove `WHERE i.is_posted` from `int_oda_ar_invoice`. Join `stg_oda__voucher_v2` to expose voucher metadata. Add two boolean flags:
- `is_invoice_posted` — the invoice itself is posted in ODA
- `is_voucher_posted` — the voucher (batch run) that generated the invoice is posted

**Rationale:** The pre-JIB use case requires unposted invoices to be visible. With explicit flag columns, every downstream model can filter however it needs — no hidden filter assumptions.

**Alternative considered:** Keep a filtered `int_oda_ar_invoice` and create a separate `int_oda_ar_invoice_all` for unposted. Rejected because it would duplicate the join logic and create confusion about which is authoritative.

### Decision 2: Skip the DRY Detail Refactor 🚫

**Chosen approach:** Keep `int_oda_ar_payments`, `int_oda_ar_adjustments`, `int_oda_ar_netting` joining staging tables directly (their existing pattern). Do **not** route them through `int_oda_ar_invoice`.

**Rationale:**
- The "duplicate" staging joins (~15 lines each) are explicit — each model declares its own dependencies. This is a feature, not a bug.
- Routing through `int_oda_ar_invoice` creates temporal coupling: if the invoice base filter changes again, transaction models break silently (inner join drops orphaned transactions).
- The DRY savings are minimal (join boilerplate) vs. the coupling risk for financial data.

**Michael's proposal note:** Michael's intent was valid — reduce repeated staging joins. But the better DRY opportunity is in the new `fct_ar_aging_detail` union, not in the intermediates.

### Decision 3: Preserve Netting Sign Convention ✅ (validated)

**Verified behavior from Snowflake:**
- `amount_applied` (payments): ODA stores as **negative** → no model-level negation needed, additive balance formula works
- `adjustment_detail_amount` (adjustments): ODA stores as **mixed sign** (negative for advance applications, positive/negative for cross-clears) → pass through, additive formula works
- `netted_amount` (netting): ODA stores as **positive** → **must negate** at model level: `-nd.netted_amount`

**Balance formula (correct, E2E validated):**
```sql
remaining_balance =
  invoice_amount          -- positive (amount billed)
  + total_payments        -- negative (reduces balance)
  + total_adjustments     -- mixed (reduces or increases based on type)
  + total_net             -- negative (negated from positive ODA values)
```

**What Michael's agg model did:** Dropped the negation. This is a bug if `-nd.netted_amount` is also removed from `int_oda_ar_netting`. In the redesign, we keep `-nd.netted_amount` in `int_oda_ar_netting` and aggregate it unchanged. The additive balance formula is preserved.

### Decision 4: Posted/Unposted Balance Split in Agg Models ✅

**Chosen approach:** Add `is_invoice_posted` and `is_voucher_posted` to GROUP BY in all three agg models:
- `int_oda_ar_invoice_payments_agg`
- `int_oda_ar_invoice_adjustments_agg`
- `int_oda_ar_invoice_netting_agg`

**Rationale:** Enables `int_oda_ar_invoice_balances` to calculate separate posted vs. unposted balance components — critical for the pre-JIB use case.

### Decision 5: Extract Remaining Balance Model ✅

**Chosen approach:** Create `int_oda_ar_invoice_remaining_balances` that:
- Computes the remaining balance formula (pulling from the 3 agg models + invoice base)
- Applies the advance/closeout pair exclusion logic (currently inline in `int_oda_ar_invoice_balances`)

**Rationale:** The current `int_oda_ar_invoice_balances` mixes two concerns: (1) balance arithmetic and (2) exclude-pair flagging. Separating them makes each model's purpose obvious.

### Decision 6: Two New Facts Replace One ✅

**Chosen approach:** Replace `fct_ar_aging` with:

**`fct_ar_aging_detail`** — transaction-level fact
- One row per transaction (invoices + payments + adjustments + netting in union)
- Includes: company, owner, well, transaction type, amount, voucher, remaining balance, `is_invoice_posted`, `is_voucher_posted`, `include_record` flag
- Materialization: `table`

**`fct_ar_aging_summary`** — invoice-level fact
- One row per invoice
- Includes: company, owner, well, invoice metadata, posted/unposted balance components, aging bucket dollar amounts
- **Aging buckets**: Standard AR (Current, 1-30 days, 31-60 days, 61-90 days, 90+ days) — computed from `DATEDIFF(day, invoice_date, CURRENT_DATE())`
- Materialization: `table`

**`fct_ar_aging`** — retire (no stable downstream consumers; novel model)

### Decision 7: Keep `dim_ar_summary` As-Is ✅

**`dim_ar_summary`** serves a different purpose — it's a raw invoice inventory joining staging directly, with the full 8-type invoice enum and statement status. It does not compute balances or aging. It complements `fct_ar_aging_summary` rather than overlapping it.

---

## Proposed New DAG

```
stg_oda__arinvoice_v2 ─┐
stg_oda__voucher_v2 ────┤
stg_oda__company_v2 ────┤──→ int_oda_ar_invoice (unfiltered, exposes is_invoice_posted, is_voucher_posted)
stg_oda__owner_v2 ──────┤
stg_oda__entity_v2 ─────┤
stg_oda__wells ─────────┘

stg_oda__arinvoicepaymentdetail ──→ int_oda_ar_payments ──→ int_oda_ar_invoice_payments_agg (GROUP BY invoice_id, is_invoice_posted, is_voucher_posted)
stg_oda__arinvoiceadjustmentdetail → int_oda_ar_adjustments → int_oda_ar_invoice_adjustments_agg (same GROUP BY)
stg_oda__arinvoicenetteddetail ──→ int_oda_ar_netting ──→ int_oda_ar_invoice_netting_agg (same GROUP BY)

int_oda_ar_invoice + all 3 agg models → int_oda_ar_invoice_balances (posted + unposted balance)
int_oda_ar_invoice_balances + int_oda_ar_advance_closeout_pairs → int_oda_ar_invoice_remaining_balances

int_oda_ar_invoice + int_oda_ar_payments + int_oda_ar_adjustments + int_oda_ar_netting → (union) → fct_ar_aging_detail
int_oda_ar_invoice_remaining_balances → fct_ar_aging_detail (joined for remaining balance + include_record)
int_oda_ar_invoice_remaining_balances → fct_ar_aging_summary (one row per invoice with aging bucket columns)
```

---

## Open Questions

1. **Voucher join in detail models**: The `is_voucher_posted` flag comes from joining `stg_oda__voucher_v2`. Do payments/adjustments/netting have their own `voucher_id` (independent of the invoice's voucher), or do they inherit the invoice's `voucher_id`? If independent, each detail model needs its own voucher join for the posted split to work correctly.

2. **Advance/closeout pair logic**: Currently `int_oda_ar_advance_closeout_pairs` uses `int_oda_ar_invoice` as its join target. With the invoice base now unfiltered, does the pairing logic still work correctly? Advances and closeouts are always posted when paired — need to verify the pairing logic doesn't pick up unposted phantom pairs.

3. **`include_record` flag in detail vs. summary**: Currently the flag is in `fct_ar_aging` at the transaction level. For the new design, should `include_record` also appear in `fct_ar_aging_summary` at the invoice level? (An invoice with `remaining_balance = 0` or `exclude_pair = true` would be `include_record = false`.)

4. **Current bucket definition**: "Current" typically means invoice date = current billing period (not overdue). What's the cutoff — current calendar month? Within 30 days of current date? This affects the aging bucket formula.

---

## What We're NOT Building (YAGNI)

- DRY refactor of staging joins in detail models — not worth the coupling risk
- Payment status tracking beyond `is_posted` — ODA doesn't expose payment lifecycle states
- Retaining `fct_ar_aging` as a deprecated alias — no consumers, clean removal

---

## Files to Create/Modify

### Modify (in-place)
- `models/operations/intermediate/finance/int_oda_ar_invoice.sql` — remove posted filter, add voucher join + flags
- `models/operations/intermediate/finance/int_oda_ar_invoice_payments_agg.sql` — add posted flags to GROUP BY
- `models/operations/intermediate/finance/int_oda_ar_invoice_adjustments_agg.sql` — same
- `models/operations/intermediate/finance/int_oda_ar_invoice_netting_agg.sql` — same
- `models/operations/intermediate/finance/int_oda_ar_invoice_balances.sql` — remove exclusion logic (moves to new model)

### Create
- `models/operations/intermediate/finance/int_oda_ar_invoice_remaining_balances.sql`
- `models/operations/marts/finance/fct_ar_aging_detail.sql`
- `models/operations/marts/finance/fct_ar_aging_summary.sql`
- YAML files for new models + updated YAML for modified models

### Retire
- `models/operations/marts/finance/fct_ar_aging.sql` — delete (no consumers)

---

## Validation Plan

1. Build `int_oda_ar_invoice` unfiltered — verify row count increases (posted + unposted)
2. Spot-check: same owner used in original Excel validation — posted-only remaining balance should match the previous `fct_ar_aging` output
3. Verify netting sign: for a known netted invoice, `remaining_balance = invoice_amount - netted_amount`
4. Verify aging buckets: pull a sample of invoices with known dates, confirm bucket assignment
5. `fct_ar_aging_summary` row count should equal distinct invoice count from `stg_oda__arinvoice_v2` (or close — some exclusions expected)
