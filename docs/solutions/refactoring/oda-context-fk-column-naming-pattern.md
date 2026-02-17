---
title: "ODA Context Docs — FK Column Naming Pattern (Drop Entity Prefix)"
category: refactoring
tags: [oda, context-docs, foreign-keys, naming-conventions, greptile, review-process]
module: context/sources/oda
symptoms:
  - Greptile confidence score 2/5 on documentation-only PR
  - FK column names in domain YAML don't match table YAML definitions
  - Entity-prefixed FK names (e.g., REVENUEDECKREVISIONID) used instead of actual column names (DECKREVISIONID)
  - Table name missing VIEW suffix in hierarchy tree
date_solved: 2026-02-17
related_prs: ["#269", "#271", "#272"]
---

# ODA Context Docs — FK Column Naming Pattern (Drop Entity Prefix)

## Problem

PR #271 fixed 6 Greptile review comments from PR #269 (ODA context documentation), but Greptile scored PR #271 at **2/5 confidence** because 4 FK column name errors remained uncorrected. The PR was merged before the review score was checked.

The errors spanned 3 files with 7 incorrect values:

| File | Documented FK | Actual FK (from table YAML) |
|------|--------------|---------------------------|
| `accounts_receivable.yaml` | `ARINVOICEADJUSTMENTID` | `INVOICEADJUSTMENTID` |
| `decks.yaml` | `REVENUEDECKREVISIONID` | `DECKREVISIONID` |
| `decks.yaml` | `EXPENSEDECKREVISIONID` | `DECKREVISIONID` |
| `oda.md` (hierarchy tree) | `ODA_BATCH_ODA_EXPENSEDECKREVISION` | `ODA_BATCH_ODA_EXPENSEDECKREVISIONVIEW` |
| `oda.md` (Key Relationships) | `ARInvoiceAdjustmentId` | `InvoiceAdjustmentId` |
| `oda.md` (Key Relationships) | `RevenueDeckRevisionId` | `DeckRevisionId` |
| `oda.md` (Key Relationships) | `ExpenseDeckRevisionId` | `DeckRevisionId` |

## Root Cause

ODA (Quorum) consistently **drops the entity prefix on child FK columns**. The parent table name disambiguates the relationship, not the column name:

| Parent Table | Child FK Column | NOT |
|-------------|----------------|-----|
| ODA_ARINVOICE_V2 | `INVOICEID` | `ARINVOICEID` |
| ODA_ARINVOICEPAYMENT | `INVOICEPAYMENTID` | `ARINVOICEPAYMENTID` |
| ODA_BATCH_ODA_ARINVOICEADJUSTMENT | `INVOICEADJUSTMENTID` | `ARINVOICEADJUSTMENTID` |
| ODA_REVENUEDECKREVISION | `DECKREVISIONID` | `REVENUEDECKREVISIONID` |
| ODA_BATCH_ODA_EXPENSEDECKREVISIONVIEW | `DECKREVISIONID` | `EXPENSEDECKREVISIONID` |
| ODA_BATCH_ODA_REVENUEDECK_V2 | `DECKSETID` | `REVENUEDECKSETID` |
| ODA_BATCH_ODA_REVENUEDECK_V2 | `DECKID` | `REVENUEDECKID` |

**Key insight**: Both revenue and expense participant tables use the **same** `DECKREVISIONID` column name. The table name disambiguates, not the column.

PR #271 correctly applied this pattern to invoice/payment FKs (`INVOICEID`, `INVOICEPAYMENTID`) but missed the adjustment detail and deck participant FKs — a partial pattern application.

## Solution

PR #272 corrected all 4 remaining errors (7 line changes across 3 files). Every FK column name was verified against the authoritative `context/sources/oda/tables/` YAML files before committing.

## Prevention: Context Doc FK Validation Checklist

When writing or reviewing ODA context documentation:

1. **Verify every FK column against the child table YAML**: Open `context/sources/oda/tables/{child_table}.yaml` and confirm the exact column name. Never guess from the parent table name.

2. **Apply naming patterns exhaustively**: When you discover a naming convention (like "drop entity prefix"), apply it to ALL relationships in the domain — not just the ones originally flagged.

3. **Cross-reference oda.md Key Relationships with domain YAMLs**: The Key Relationships table in `oda.md` and the Relationships block in each domain YAML must agree. Update both when fixing either.

4. **Check Greptile score before merging**: A score of 2/5 or lower means substantive errors remain. Do not merge until issues are addressed.

5. **Watch for VIEW suffix**: `ODA_BATCH_ODA_EXPENSEDECKREVISIONVIEW` is the correct table (Quorum convenience view). The bare `ODA_BATCH_ODA_EXPENSEDECKREVISION` is an old-connector table. Always include the VIEW suffix in documentation.

## Related Documentation

- [ODA Context Documentation — Sprint 0 Source Audit Methodology](oda-context-documentation-sprint-0.md) — the methodology that produced PR #269
- `context/sources/oda/tables/` — authoritative column-level source of truth for FK verification
