# ODA Analytics Buildout — Scope Validation Brainstorm

**Date:** 2026-02-20
**Linear Project:** [ODA Analytics Buildout](https://linear.app/formentera-ops/project/oda-analytics-buildout-d7f8df21f33c)
**Status:** Planned (all 13 issues in Backlog)

---

## What We're Building

Full gold-layer mart buildout on top of the 48-model ODA staging layer, organized into three milestones:

1. **M1: Context & Documentation** — Enrich ODA context docs with unstaged table YAMLs, validate existing docs against ODA Data Hub PDF
2. **M2: Revenue Staging Sprint** — Stage the 4 unstaged revenue tables needed to unlock fct_revenue_distribution
3. **M3: Mart Buildout** — 6 gold-layer facts + 2 platinum OBTs + 1 bridge + 1 legacy refactor

The project delivers the complete ODA financial analytics stack — from JIB partner billing through revenue distribution to well-level net economics P&L.

---

## Current Foundation (Already Built)

| Layer | Count | Status |
|-------|-------|--------|
| Staging models | 48 | 100% complete (5-CTE, all documented) |
| Intermediates | 14 | Finance domain (GL, AR) |
| Gold marts | 5 | AR aging detail/summary, 3 check registers |
| Master data dims | 8 | accounts, AFEs, companies, entities, owners, purchasers, vendors, wells |
| Context docs | 89 files | Full coverage in `context/sources/oda/` |

---

## Scope Changes (vs. Original Linear Issues)

### Add (3 new issues)

| Issue | Milestone | Rationale |
|-------|-----------|-----------|
| **Stage APCheckDetail** | M2 | FOR-293 (fct_ap_payment_activity) needs check-to-invoice allocation detail. Without it, can only show check-level totals, not which invoices a check paid. |
| **Validation gate for large-table marts** | Pre-M3 | FK integrity, row counts, dedup checks on JIB (63M), deck participants (196M), and revenue tables BEFORE building marts. Lessons from Sprint 3 stim failure (missing source tables) and userfield dedup bug. |
| **Refactor dim_company_WI/NRI to source from fct_deck_interest_history** | M3 | Legacy current-snapshot models (dim_company_WI, dim_company_NRI, fct_company_WI_NRI_calc) should become views off the new canonical fact. Preserves Eduardo/Christian's ComboCurve join while eliminating parallel data paths. |

### Remove (1 issue)

| Issue | Reason |
|-------|--------|
| **FOR-296 (deprecate lease_operating_statement)** | Already deleted. Close the issue. |

### Modify (1 issue)

| Issue | Change |
|-------|--------|
| **FOR-292 (fct_deck_interest_history + bridge_well_owner)** | Clarify scope: covers BOTH revenue and expense decks in a unified fact with `deck_type` discriminator column (revenue/expense). Revenue rows carry `product_id`; expense rows have `product_id = NULL`. |

### Defer (out of scope)

| Item | Rationale |
|------|-----------|
| Cortex Analyst semantic layer | Separate project. This project delivers tables; semantic models involve different stakeholders. |
| Owner-to-HubSpot automated link | No current automated reconciliation between `dim_owners` (ODA) and `dim_owner` (HubSpot). Future effort. |
| WellView agreements-to-ODA decks | Conceptual overlap (both track WI/ORRI). No automated link exists or is planned for this project. |

---

## Key Design Decisions

### 1. Unified Deck Interest History (Revenue + Expense)

**Decision:** Single `fct_deck_interest_history` covers both revenue and expense deck participants.

- `deck_type` column discriminates revenue vs. expense
- Revenue rows carry `product_id` (Oil/Gas/NGL/etc.); expense rows have NULL
- Revenue deck sources `stg_oda__revenue_deck_participant` (196M rows)
- Expense deck sources `stg_oda__expense_deck_participant` (877K rows)

**Why unified:** `bridge_well_owner` needs one authoritative source for "who has interest in this well." Splitting would require UNION in the bridge, adding complexity.

**Open question for planning:** Revenue participants reference `OwnerId`; expense participants may reference `EntityId`. Need to verify the FK and decide whether to normalize to a single `interest_holder_id` column or carry both.

### 2. Legacy WI/NRI Refactored as Views

**Decision:** Keep `dim_company_WI`, `dim_company_NRI`, and `fct_company_WI_NRI_calc` but refactor them to source from `fct_deck_interest_history` instead of directly from intermediates.

- Current pattern: `int_oda_latest_company_WI` → `dim_company_WI` → INNER JOIN `stg_cc__company_wells`
- Future pattern: `fct_deck_interest_history` (filtered to latest effective + expense deck) → `dim_company_WI` (view) → INNER JOIN `stg_cc__company_wells`
- Preserves Eduardo/Christian's workflow and ComboCurve enrichment
- Eliminates the parallel data path through `int_oda_latest_company_WI/NRI`

### 3. APCheckDetail Staging Unlocks Invoice-Level Reconciliation

**Decision:** Add APCheckDetail staging to M2 so `fct_ap_payment_activity` can show which invoices each check paid.

Without it: check-level totals only (aggregate). With it: full invoice-to-check allocation for AP reconciliation.

### 4. Validation Gate Before Large-Table Marts

**Decision:** Add a validation issue to run before building marts against JIB (63M), deck participants (196M+), and revenue tables.

Checks to run:
- Row counts match expectations
- FK integrity between parent/child tables
- Dedup verification (QUALIFY-based dedup works correctly)
- Source table existence in `information_schema.tables` for any newly staged revenue tables
- CDC vs batch classification confirmed

### 5. Platinum OBTs Stay In-Scope

**Decision:** Both `plat_well__net_economics` and `plat_owner__revenue_statement` remain in this project. They're the capstone — the entire M2 revenue staging sprint exists to enable them.

---

## Revised Issue Inventory

### M1: Context & Documentation (4 issues, unchanged)

| ID | Title | Priority |
|----|-------|----------|
| FOR-283 | Add unstaged table YAMLs from ODA Data Hub PDF | High |
| FOR-284 | Update _index.yaml with unstaged table catalog | Medium |
| FOR-285 | Annotate stg_oda__wells.yaml — PropertyReferenceCode is the EID | High |
| FOR-286 | Validate existing 46 ODA table YAMLs against Data Hub PDF | Medium |

### M2: Revenue Staging Sprint (3 existing + 1 new = 4 issues)

| ID | Title | Priority |
|----|-------|----------|
| FOR-287 | Stage OwnerRevenueDetail_V2 | Urgent |
| FOR-288 | Stage CheckRevenueDetail_V2 | High |
| FOR-289 | Stage RevenueDeckDeduction + RevenueDeductionType_V2 | Medium |
| **NEW** | Stage APCheckDetail | Medium |

### M2.5: Validation Gate (1 new issue)

| ID | Title | Priority |
|----|-------|----------|
| **NEW** | Validate FK integrity and dedup on JIB, deck, and revenue tables | High |

### M3: Mart Buildout (5 existing + 1 new + 1 modified = 7 issues)

| ID | Title | Priority | Notes |
|----|-------|----------|-------|
| FOR-290 | Build fct_jib_billing | High | |
| FOR-291 | Build fct_cost_vs_budget | High | Active business need (drilling cost control) |
| FOR-292 | Build fct_deck_interest_history + bridge_well_owner | High | **Modified:** covers both revenue + expense decks |
| FOR-293 | Build fct_ap_payment_activity | Medium | Now depends on APCheckDetail staging |
| FOR-294 | Build fct_revenue_distribution | Urgent | |
| FOR-295 | Build plat_well__net_economics + plat_owner__revenue_statement | High | |
| **NEW** | Refactor dim_company_WI/NRI to source from fct_deck_interest_history | Medium | |

### Removed

| ID | Title | Reason |
|----|-------|--------|
| ~~FOR-296~~ | ~~Deprecate lease_operating_statement~~ | Already deleted |

---

## Open Questions (For Planning Phase)

1. **Deck participant FK difference:** Do expense deck participants use `OwnerId` or `EntityId`? This affects the unified fact schema.
2. **APCheckDetail connector type:** Is it batch or CDC? Does it exist in `ESTUARY_DB` at all? Need `information_schema.tables` validation.
3. **JIB/JIBDetail relationship:** entity_model.md says they're "parallel denormalized" with no FK. How does `fct_jib_billing` join them — or does it only need JIBDetail?
4. **`fct_cost_vs_budget` join strategy:** GL (180M) × AFE budget detail (424K). What's the join key? AFE ID + account + period? Needs investigation during planning.
5. **Revenue deck participant volume:** 196M rows for the full history fact. Materialization? Incremental with what watermark? Or table with clustering?
6. **`plat_well__net_economics` LOE source:** Uses `fct_los` for costs. Does it also need `general_ledger` for non-LOS costs, or is LOS the complete cost view?

---

## Dependencies Graph

```
M1: Context & Documentation
  └── FOR-283 (unstaged YAMLs) ──── soft prereq ───→ M2 staging work

M2: Revenue Staging Sprint
  ├── FOR-287 (OwnerRevenueDetail_V2) ──→ FOR-294 (fct_revenue_distribution)
  ├── FOR-288 (CheckRevenueDetail_V2) ──→ FOR-294 (enrichment)
  ├── FOR-289 (RevenueDeckDeduction) ──→ FOR-294 (deduction detail)
  └── NEW (APCheckDetail) ──→ FOR-293 (fct_ap_payment_activity)

M2.5: Validation Gate
  └── Run BEFORE M3 mart construction

M3: Mart Buildout (parallelizable where shown)
  ├── FOR-290 (fct_jib_billing) ──── independent, can start anytime
  ├── FOR-291 (fct_cost_vs_budget) ──── independent, can start anytime
  ├── FOR-292 (fct_deck_interest_history + bridge) ──── independent
  │     └── NEW (refactor dim_company_WI/NRI) ──── depends on FOR-292
  ├── FOR-293 (fct_ap_payment_activity) ──── depends on APCheckDetail staging
  ├── FOR-294 (fct_revenue_distribution) ──── depends on M2 complete
  └── FOR-295 (platinum OBTs) ──── depends on FOR-294 + fct_los
```

**Parallelism opportunity:** FOR-290, FOR-291, FOR-292 have no staging prereqs — they can start as soon as the validation gate passes.

---

## Investigation Findings (M3-Sprint-2)

### AFEBudgetEntry Table Missing — fct_cost_vs_budget Blocked (FOR-291)

**Investigated:** 2026-02-20

The FK chain for budget data is: `AFEBudgetDetail_V2.AFEBUDGETENTRYID` → `AFEBudgetEntry` (missing) → `AFEBudget.ID` → `AFE.ID`.

- `AFEBudgetEntry` does NOT exist in `ESTUARY_DB.INFORMATION_SCHEMA.TABLES` (validated 2026-02-20)
- `AFEBudgetDetail_V2` has 435K rows with monthly amounts (AMOUNT, MONTH) but they connect to AFE only through the missing bridge table
- `AFEBudget` has 1,958 header rows with `AFEID`, `WELLID`, `FISCALYEAR` but NO dollar amounts
- Attempted join: 0 of 36,289 distinct `AFEBUDGETENTRYID` values match `AFEBudget.ID` — confirmed different entity levels

**Impact:** Cannot build fct_cost_vs_budget until Estuary syncs `AFEBudgetEntry`. The table carries `ACCOUNTID` (for account-level breakdown) and is the bridge between monthly amounts and AFE headers.

**Resolution:** FOR-291 deferred. Requires Estuary admin to add `AFEBudgetEntry` to the ODA batch connector.

---

## Next Step

Run `/workflows:plan` to convert this into sprint-level implementation plans with acceptance criteria.
